package cn.pandadb.server

import java.io.{File, FileInputStream}
import java.util.{Optional, Properties}
import java.util.concurrent.CountDownLatch

import cn.pandadb.context.InstanceBoundServiceFactoryRegistry
import cn.pandadb.cypherplus.SemanticOperatorServiceFactory
import cn.pandadb.externalprops.CustomPropertyNodeStoreHolderFactory
import cn.pandadb.network.{ClusterClient, NodeAddress, ZKPathConfig, ZookeeperBasedClusterClient}
import cn.pandadb.server.internode.InterNodeRequestHandler
import cn.pandadb.server.neo4j.Neo4jRequestHandler
import cn.pandadb.server.rpc.{NettyRpcServer, PNodeRpcClient}
import cn.pandadb.util.Ctrl._
import cn.pandadb.util.{ContextMap, Logging}
import org.apache.commons.io.IOUtils
import org.apache.curator.framework.recipes.leader.{LeaderSelector, LeaderSelectorListenerAdapter}
import org.apache.curator.framework.CuratorFramework
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

  def startServer(dbDir: File, configFile: File, configOverrides: Map[String, String] = Map()): PNodeServer = {
    val server = new PNodeServer(dbDir, configFile, configOverrides);
    server.start();
    server;
  }
}

object PNodeServerContext extends ContextMap {

  def bindStoreDir(storeDir: File): Unit = {
    GlobalContext.put[File]("pnode.store.dir", storeDir)
  }

  def bindRpcPort(port: Int): Unit = {
    this.put("pnode.rpc.port", port)
  }

  def bindLocalIpAddress(address: String): Unit = {
    this.put("pnode.local.ipAddress", address)
  }

  def bindJsonDataLog(jsonDataLog: JsonDataLog): Unit = {
    this.put[JsonDataLog]("pnode.dataLog", jsonDataLog)
  }

  def bindMasterRole(masterRole: MasterRole): Unit =
    this.put[MasterRole](masterRole)

  def bindClusterClient(client: ClusterClient): Unit =
    this.put[ClusterClient](client)

  def getMasterRole: MasterRole = this.get[MasterRole]

  def getClusterClient: ClusterClient = this.get[ClusterClient]

  def getStoreDir: File = GlobalContext.get[File]("pnode.store.dir")

  def getJsonDataLog: JsonDataLog = this.get[JsonDataLog]("pnode.dataLog")

  def getRpcPort: Int = this.get[Int]("pnode.rpc.port")

  def getLocalIpAddress: String = this.get[String]("pnode.local.ipAddress")

  def bindLeaderNode(boolean: Boolean): Unit =
    this.put("is.leader.node", boolean)

  def isLeaderNode: Boolean = this.getOption("is.leader.node").getOrElse(false)
}


/*
new mechanism after discussion in 12/03
1. zk: keep version and (lastFreshNode)masterNode address,
    only leader can write this info to zk.
    leader check the zk, if a version num not found, init it.
2. server: wait for available(lastFreshNode) node back.
3. how to get log from leader (how to use RPC)
4. if a node is not fresh, it shouldn't join in selectLeader.
 */
class PNodeServer(dbDir: File, configFile: File, configOverrides: Map[String, String] = Map())
  extends LeaderSelectorListenerAdapter with Logging {
  //TODO: we will replace neo4jServer with InterNodeRpcServer someday!!
  val neo4jServer = new CommunityBootstrapper();
  val runningLock = new CountDownLatch(1)

  //prepare args for ZKClusterClient
  val props = new Properties()
  props.load(new FileInputStream(configFile))
  val zkString: String = props.getProperty("zkServerAddress")
  val clusterClient: ZookeeperBasedClusterClient = new ZookeeperBasedClusterClient(zkString)
  val client = clusterClient.curator
  var masterRole: MasterRole = null
  PNodeServerContext.bindLocalIpAddress(props.getProperty("localIpAddress"))

  // host?
  PNodeServerContext.bindRpcPort(props.getProperty("rpcPort").toInt)
  val serverKernel = new NettyRpcServer("0.0.0.0", PNodeServerContext.getRpcPort, "inter-node-server");
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

//    new Thread() {
//      override def run() {
//        neo4jServer.start(dbDir, Optional.of(configFile),
//          JavaConversions.mapAsJavaMap(configOverrides));
//      }
//    }.start()
    neo4jServer.start(dbDir, Optional.of(configFile),
      JavaConversions.mapAsJavaMap(configOverrides));

    serverKernel.start({
      //scalastyle:off
      println(PNodeServer.logo);

      ZKPathConfig.initZKPath(clusterClient.curator)
      PNodeServerContext.bindJsonDataLog(_getJsonDataLog())
      if(_isUpToDate() == false){
        _updataLocalData()
      }
      _joinInLeaderSelection()
      new ZKServiceRegistry(zkString).registerAsOrdinaryNode(props.getProperty("localNodeAddress"))

    });

  }

  def shutdown(): Unit = {
    runningLock.countDown()
    serverKernel.shutdown();
  }

  override def takeLeadership(curatorFramework: CuratorFramework): Unit = {
    PNodeServerContext.bindLeaderNode(true);

    // here to init master role
    new ZKServiceRegistry(zkString).registerAsLeader(props.getProperty("localNodeAddress"))
    masterRole = new MasterRole(clusterClient, NodeAddress.fromString(props.getProperty("localNodeAddress")))
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

    val localDriver = GraphDatabase.driver(s"bolt://" + props.getProperty("localNodeAddress"))
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