package cn.pandadb.server

import java.io.{File, FileInputStream}
import java.util.concurrent.CountDownLatch
import java.util.{Optional, Properties}

import cn.pandadb.blob.BlobStorageModule
import cn.pandadb.cypherplus.CypherPlusModule
import cn.pandadb.externalprops.ExternalPropertiesModule
import cn.pandadb.network.{NodeAddress, ZKPathConfig, ZookeeperBasedClusterClient}
import cn.pandadb.server.internode.InterNodeRequestHandler
import cn.pandadb.server.neo4j.Neo4jRequestHandler
import cn.pandadb.server.rpc.{NettyRpcServer, PNodeRpcClient}
import cn.pandadb.util._
import org.apache.commons.io.IOUtils
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.leader.{LeaderSelector, LeaderSelectorListenerAdapter}
import org.neo4j.driver.GraphDatabase
import org.neo4j.server.CommunityBootstrapper

import scala.collection.JavaConversions

/**
  * Created by bluejoe on 2019/7/17.
  */
object PNodeServer extends Logging {
  val logo = IOUtils.toString(this.getClass.getClassLoader.getResourceAsStream("logo.txt"), "utf-8");

  def startServer(dbDir: File, configFile: File, overrided: Map[String, String] = Map()): PNodeServer = {
    val props = new Properties()
    props.load(new FileInputStream(configFile))
    val server = new PNodeServer(dbDir, JavaConversions.propertiesAsScalaMap(props).toMap ++ overrided);
    server.start();
    server;
  }
}

class PNodeServer(dbDir: File, props: Map[String, String])
  extends LeaderSelectorListenerAdapter with Logging {
  //TODO: we will replace neo4jServer with InterNodeRpcServer someday!!
  val neo4jServer = new CommunityBootstrapper();
  val runningLock = new CountDownLatch(1)

  val modules = new PandaModules();
  val context = new ContextMap();

  val config = new Configuration() {
    override def getRaw(name: String): Option[String] = props.get(name)
  }

  val pmc = PandaModuleContext(config, dbDir, context);

  modules.add(new MainServerModule())
    .add(new BlobStorageModule())
    .add(new ExternalPropertiesModule())
    .add(new CypherPlusModule())

  modules.init(pmc);

  val np = MainServerContext.nodeAddress

  val serverKernel = new NettyRpcServer("0.0.0.0", MainServerContext.rpcPort, "PNodeRpc-service");
  serverKernel.accept(Neo4jRequestHandler());
  serverKernel.accept(InterNodeRequestHandler());

  val dataLogRW: JsonDataLogRW = {
    val logFile = new File(dbDir, "dataVersionLog.json")
    if (!logFile.exists) {
      logFile.getParentFile.mkdirs()
      logFile.createNewFile()
    }
    new JsonDataLogRW(logFile)
  }

  MainServerContext.bindDataLogReaderWriter(dataLogRW, dataLogRW)
  val clusterClient: ZookeeperBasedClusterClient = new ZookeeperBasedClusterClient(MainServerContext.zkServerAddressStr)

  def start(): Unit = {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      override def run(): Unit = {
        shutdown();
      }
    });

    neo4jServer.start(dbDir, Optional.empty(),
      JavaConversions.mapAsJavaMap(props + ("dbms.connector.bolt.listen_address" -> np.getAsString)));

    modules.start(pmc);
    //FIXME: watch dog is not a PNode
    if (!GlobalContext.isWatchDog()) {
      serverKernel.start({
        //scalastyle:off
        println(PNodeServer.logo);

        if (_isUpToDate() == false) {
          _updateLocalData()
        }
        _joinInLeaderSelection()
        new ZKServiceRegistry(MainServerContext.zkServerAddressStr).registerAsOrdinaryNode(np)
      })
    }
  }

  def shutdown(): Unit = {
    modules.close(pmc);
    runningLock.countDown()
    serverKernel.shutdown();
  }

  override def takeLeadership(curatorFramework: CuratorFramework): Unit = {

    new ZKServiceRegistry(MainServerContext.zkServerAddressStr).registerAsLeader(np)
    val masterRole = new MasterRole(clusterClient, np)
    MainServerContext.bindMasterRole(masterRole)

    logger.debug(s"taken leader ship...");
    //yes, i won't quit, never!
    runningLock.await()
    logger.debug(s"shutdown...");
  }

  private def _joinInLeaderSelection(): Unit = {
    val leaderSelector = new LeaderSelector(clusterClient.curator, ZKPathConfig.registryPath + "/_leader", this);
    leaderSelector.start();
  }

  private def _isUpToDate(): Boolean = {
    dataLogRW.getLastVersion() == clusterClient.getClusterDataVersion()
  }

  //FIXME: updata->update
  private def _updateLocalData(): Unit = {
    // if can't get now, wait here.
    val cypherArray = _getRemoteLogs()

    val localDriver = GraphDatabase.driver(s"bolt://${np.getAsString}")
    val session = localDriver.session()
    cypherArray.foreach(logItem => {
      val tx = session.beginTransaction()
      try {
        val localPreVersion = dataLogRW.getLastVersion()
        tx.run(logItem.command)
        tx.success()
        tx.close()
        dataLogRW.write(logItem)
      }
    })
  }

  // todo: Iterable[]
  private def _getRemoteLogs(): Iterable[DataLogDetail] = {
    val lastFreshNodeIP = clusterClient.getFreshNodeIp()
    val rpcClient = PNodeRpcClient.connect(NodeAddress.fromString(lastFreshNodeIP))
    rpcClient.getRemoteLogs(dataLogRW.getLastVersion())
  }
}