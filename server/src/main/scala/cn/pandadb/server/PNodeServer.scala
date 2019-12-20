package cn.pandadb.server

import java.io.{File, FileInputStream}
import java.util.{Optional, Properties}
import java.util.concurrent.CountDownLatch

import cn.pandadb.context.InstanceBoundServiceFactoryRegistry
import cn.pandadb.cypherplus.SemanticOperatorServiceFactory
import cn.pandadb.externalprops.CustomPropertyNodeStoreHolderFactory
import cn.pandadb.network.{NodeAddress, ZKPathConfig, ZookeeperBasedClusterClient}
import cn.pandadb.server.internode.InterNodeRequestHandler
import cn.pandadb.server.neo4j.Neo4jRequestHandler
import cn.pandadb.server.rpc.{NettyRpcServer, PNodeRpcClient}
import cn.pandadb.util.Ctrl._
import cn.pandadb.util.Logging
import org.apache.commons.io.IOUtils
import org.apache.curator.framework.recipes.leader.{LeaderSelector, LeaderSelectorListenerAdapter}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.neo4j.driver.GraphDatabase
import org.neo4j.kernel.impl.blob.{BlobStorageServiceFactory, DefaultBlobFunctionsServiceFactory}
import org.neo4j.server.CommunityBootstrapper

import scala.collection.JavaConversions

/**
  * Created by bluejoe on 2019/7/17.
  */
object PNodeServer extends Logging {
  val logo = IOUtils.toString(this.getClass.getClassLoader.getResourceAsStream("logo.txt"), "utf-8");

  run("registering global database lifecycle service") {
    InstanceBoundServiceFactoryRegistry.register[BlobStorageServiceFactory];
    InstanceBoundServiceFactoryRegistry.register[DefaultBlobFunctionsServiceFactory];
    InstanceBoundServiceFactoryRegistry.register[SemanticOperatorServiceFactory];
    InstanceBoundServiceFactoryRegistry.register[CustomPropertyNodeStoreHolderFactory];
  }

  def startServer(dbDir: File, configFile: File, overrided: Map[String, String] = Map()): PNodeServer = {
    val props = new Properties()
    props.load(new FileInputStream(configFile))
    val server = new PNodeServer(dbDir, JavaConversions.propertiesAsScalaMap(props).toMap ++ overrided);
    server.start();
    server;
  }
}

class PNodeServer(dbDir: File, props: Map[String, String] = Map())
  extends LeaderSelectorListenerAdapter with Logging {
  //TODO: we will replace neo4jServer with InterNodeRpcServer someday!!
  val neo4jServer = new CommunityBootstrapper();
  val runningLock = new CountDownLatch(1)

  //prepare args for ZKClusterClient
  PNodeServerContext.putAll(props)
  import cn.pandadb.util.ConfigUtils._

  val zkString: String = props.getRequiredValueAsString("zookeeper.address")
  private val _tempCurator = CuratorFrameworkFactory.newClient(zkString,
    new ExponentialBackoffRetry(1000, 3))
  _tempCurator.start()
  ZKPathConfig.initZKPath(_tempCurator)
  _tempCurator.close()
  val clusterClient: ZookeeperBasedClusterClient = new ZookeeperBasedClusterClient(zkString)
  val client = clusterClient.curator
  var masterRole: MasterRole = null

  val np = NodeAddress.fromString(props.getRequiredValueAsString("node.server.address"))
  //TOOD: bindNodeAddress
  PNodeServerContext.bindLocalIpAddress(np.host)
  PNodeServerContext.bindRpcPort(np.port)

  val serverKernel = new NettyRpcServer("0.0.0.0", PNodeServerContext.getRpcPort, "PNodeRpc-service");
  serverKernel.accept(Neo4jRequestHandler());
  serverKernel.accept(InterNodeRequestHandler());
  PNodeServerContext.bindStoreDir(dbDir)

  def start(): Unit = {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      override def run(): Unit = {
        shutdown();
      }
    });

    PNodeServerContext.bindClusterClient(clusterClient);

    neo4jServer.start(dbDir, Optional.empty(),
     JavaConversions.mapAsJavaMap(props + ("dbms.connector.bolt.listen_address" -> np.getAsString)));

    serverKernel.start({
      //scalastyle:off
      println(PNodeServer.logo);

      PNodeServerContext.bindJsonDataLog(_getJsonDataLog())
      if(_isUpToDate() == false){
        _updataLocalData()
      }
      _joinInLeaderSelection()
      new ZKServiceRegistry(zkString).registerAsOrdinaryNode(np)

    });

  }

  def shutdown(): Unit = {
    runningLock.countDown()
    serverKernel.shutdown();
  }

  override def takeLeadership(curatorFramework: CuratorFramework): Unit = {
    PNodeServerContext.bindLeaderNode(true);

    new ZKServiceRegistry(zkString).registerAsLeader(np)
    masterRole = new MasterRole(clusterClient, np)
    PNodeServerContext.bindMasterRole(masterRole)

    logger.debug(s"taken leader ship...");
    //yes, i won't quit, never!
    runningLock.await()
    logger.debug(s"shutdown...");
  }

  private def _joinInLeaderSelection(): Unit = {
    val leaderSelector = new LeaderSelector(client, ZKPathConfig.registryPath + "/_leader", this);
    leaderSelector.start();
  }

  private def _isUpToDate(): Boolean = {
    PNodeServerContext.getJsonDataLog.getLastVersion() == clusterClient.getClusterDataVersion()
  }

  private def _updataLocalData(): Unit = {
    // if can't get now, wait here.
    val cypherArr = _getRemoteLogs()

    val localDriver = GraphDatabase.driver(s"bolt://" + props.get("localNodeAddress"))
    val session = localDriver.session()
    cypherArr.foreach(logItem => {
      val tx = session.beginTransaction()
      try {
        val localPreVersion = PNodeServerContext.getJsonDataLog.getLastVersion()
        tx.run(logItem.command)
        tx.success()
        tx.close()
        PNodeServerContext.getJsonDataLog.write(logItem)
      }
    })
  }

  private def _getRemoteLogs(): Array[DataLogDetail] = {
    val lastFreshNodeIP = clusterClient.getFreshNodeIp()
    val rpcClient = PNodeRpcClient.connect(lastFreshNodeIP)
    rpcClient.getRemoteLogs(PNodeServerContext.getJsonDataLog.getLastVersion())
  }

  private def _getJsonDataLog(): JsonDataLog = {
    val logFilePath = PNodeServerContext.getStoreDir.getPath + "/dataVersionLog.json"
    val storeDir = new File(PNodeServerContext.getStoreDir.getPath)
    val logFile = new File(logFilePath)
    if (logFile.exists() == false) {
      storeDir.mkdirs()
      logFile.createNewFile()
    }
    new JsonDataLog(logFile)
  }

}