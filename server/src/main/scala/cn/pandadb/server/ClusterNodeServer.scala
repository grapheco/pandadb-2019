package cn.pandadb.server

import cn.pandadb.cluster.ClusterService
import cn.pandadb.configuration.Config
import cn.pandadb.leadernode.LeaderNodeDriver
import cn.pandadb.server.Store.DataStore
import cn.pandadb.server.modules.LifecycleServerModule
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.HippoRpcEnvFactory
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnvClientConfig}
import org.apache.curator.shaded.com.google.common.net.HostAndPort

class ClusterNodeServer(config: Config, clusterService: ClusterService, dataStore: DataStore)
      extends LifecycleServerModule{
  val logger = config.getLogger(this.getClass)
  val nodeHostAndPort = HostAndPort.fromString(config.getNodeAddress())

  override def start(): Unit = {
    logger.info(this.getClass + ": start")

    //clusterService.doNodeStart2()
    logger.info(this.getClass + ": doNodeStart")
    clusterService.registerListenner()
    //clusterService.registerAsOnLineNode()
    clusterService.assureLeaderExist()
    if(!clusterService.isLeaderNode()) {
      if (clusterService.inElection) {
        if (clusterService.leaderLatch != null) clusterService.leaderLatch.close()
        clusterService.leaderLatch = null
        clusterService.inElection = false
      }
      clusterService.tryToBecomeDataNode()
    }

/*    if (clusterService.hasLeaderNode()) {
      val leaderNode = clusterService.getLeaderNodeHostAndPort()
      syncDataFromCluster(leaderNode)
    }
    clusterService.registerAsFreshNode()
    while (!clusterService.hasLeaderNode()) {
      participateInLeaderElection()
    }*/
  }

  def getLocalDataVersion(): String = {
    //todo
    null
  }

  def becomeDataNode(): Boolean = {
    null
  }
  def tryToBecomeDataNode(): Unit = {
    //todo
    logger.info(this.getClass + ": tryToBecomeDataNode" + nodeHostAndPort)
    var isDataNode = false
    while (!isDataNode) {
      syncDataFromCluster(HostAndPort.fromString(clusterService.getLeaderNodeAddress()))
      //dataVersion = clusterService.getDataVersion()
      //localDataVersion = getLocalDataVersion()
      if (becomeDataNode()) {
        isDataNode = true
      }
    }
    //todo should init something for read and write data
    //clusterService.lockDataVersion()
    //clusterService.registerAsDataNode()

  }

  def syncDataFromCluster(leaderNode: HostAndPort): Unit = {
    logger.info("syncDataFromCluster")
      val dbDir = dataStore.databaseDirectory

      val rpcServerName = config.getRpcServerName()
      val LeaderNodeEndpointName = config.getLeaderNodeEndpointName()
      val clientConfig = RpcEnvClientConfig(new RpcConf(), rpcServerName)
      val clientRpcEnv = HippoRpcEnvFactory.create(clientConfig)
      val leaderNodeEndpointRef = clientRpcEnv.setupEndpointRef(
        new RpcAddress(leaderNode.getHostText, leaderNode.getPort), LeaderNodeEndpointName)
      val leaderNodeDriver = new LeaderNodeDriver
//      leaderNodeDriver.pullDbFileFromDataNode()
      logger.info(s"syncDataFromCluster: pull data <fromVersion: ${dataStore.getDataVersion()}> to dir <$dbDir>")
      logger.info("update local data version")
      dataStore.setDataVersion(0)
  }

  def participateInLeaderElection(): Unit = {
    logger.info(this.getClass + "participateInLeaderElection: " )
    val localDataVersion = dataStore.getDataVersion()
    val clusterDataVersion = clusterService.getDataVersion().toLong
    if (localDataVersion >= clusterDataVersion) {
      // try register leader
      // add leader lister handler function

    }
  }
}
