package cn.pandadb.server

import cn.pandadb.cluster.ClusterService
import cn.pandadb.configuration.Config
import cn.pandadb.leadernode.LeaderNodeDriver
import cn.pandadb.server.Store.DataStore
import cn.pandadb.server.modules.LifecycleServerModule
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.HippoRpcEnvFactory
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnvClientConfig}
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.{PathChildrenCacheEvent, PathChildrenCacheListener}
import org.apache.curator.framework.recipes.leader.{LeaderLatch, LeaderLatchListener}
import org.apache.curator.shaded.com.google.common.net.HostAndPort

class ClusterNodeServer(config: Config, clusterService: ClusterService, dataStore: DataStore)
      extends LifecycleServerModule{
  val logger = config.getLogger(this.getClass)
  val nodeHostAndPort = HostAndPort.fromString(config.getNodeAddress())
  val nodeAddress = config.getNodeAddress()
  var inElection = false
  var leaderLatch: LeaderLatch = null

  override def start(): Unit = {
    logger.info(this.getClass + ": start")
    logger.info(this.getClass + ": doNodeStart")
    registerListenner()
    assureLeaderExist()
    if(!clusterService.isLeaderNode()) {
      if (inElection) {
        if (clusterService.leaderLatch != null) leaderLatch.close()
        clusterService.leaderLatch = null
        inElection = false
      }
      tryToBecomeDataNode()
    }
  }

  def getLocalDataVersion(): String = {
    //todo
    null
  }

  def registerListenner(): Unit = {
    //listen leaderNode
    val leaderNodeCacheListener = new PathChildrenCacheListener {
      override def childEvent(curatorFramework: CuratorFramework, pathChildrenCacheEvent: PathChildrenCacheEvent): Unit = {
        val eventType = pathChildrenCacheEvent.getType
        eventType match {
          case PathChildrenCacheEvent.Type.CHILD_REMOVED => {
            if (!clusterService.isLeaderNode()) {
              assureLeaderExist()
              tryToBecomeDataNode()
            }
          }
          case _ => null
        }
      }
    }
    clusterService.registerPathChildrenListener(clusterService.leaderNodesPath, leaderNodeCacheListener)
  }
  def assureLeaderExist(): Unit = {

    var leaderNodeAddress = clusterService.getLeaderNodeAddress()
    var dataVersion = clusterService.getDataVersion()
    val localDataVersion = getLocalDataVersion()

    while (leaderNodeAddress == null) {
      logger.info(this.getClass + ": cluster has no leader")
      if (dataVersion.toInt <= localDataVersion.toInt && !inElection) {
        inElection = true
        participateInLeaderElection()
      }
      logger.info("dataVersion ==" + dataVersion)
      logger.info("localdataVersion ==" + localDataVersion)
      logger.info("election ==" + inElection)
      logger.info("==== wait getLeaderNode ====")
      Thread.sleep(500)
      leaderNodeAddress = clusterService.getLeaderNodeAddress()
      dataVersion = clusterService.getDataVersion()
    }
    logger.info(this.getClass + ": cluster has a new leader: " + leaderNodeAddress)
    logger.info(this.getClass + ": dataVersion: " + dataVersion)

  }

  def tryToBecomeDataNode(): Unit = {
    logger.info(this.getClass + ": tryToBecomeDataNode" + nodeHostAndPort)
    while (!clusterService.checkIsDataNode(nodeAddress)) {
      var dataVersion = clusterService.getDataVersion()
      var localDataVersion = getLocalDataVersion()
      if (dataVersion.toInt > localDataVersion.toInt) {
        syncDataFromCluster(HostAndPort.fromString(clusterService.getLeaderNodeAddress()))
      }
      clusterService.lockDataVersion()
      dataVersion = clusterService.getDataVersion()
      localDataVersion = getLocalDataVersion()
      if (dataVersion.toInt > localDataVersion.toInt) {
        syncDataFromCluster(HostAndPort.fromString(clusterService.getLeaderNodeAddress()))
      }
      clusterService.registerAsDataNode()
      clusterService.unLockDataVersion()
    }
    if (!inElection) {
      inElection = true
      participateInLeaderElection()
    }
  }

  def updateLocalDataToLatestVersion(): Unit = {
    //todo
    logger.info(this.getClass + ": syncLocalGraphData")
    logger.info(this.getClass + ": dataVersion = " + clusterService.getDataVersion())
    logger.info(this.getClass + ": localdataVersion = " + getLocalDataVersion())
    val leaderNodeAddress = clusterService.getLeaderNodeAddress()
    logger.info("LeaderNode: " + leaderNodeAddress)
    logger.info("pull ")
    //localDataVersion = (localDataVersion.toInt + 1 ).toString
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
    logger.info(this.getClass + "participateInLeaderElection: " + nodeAddress)
    val finalLeaderLatch = new LeaderLatch(clusterService.curator, clusterService.leaderLatchPath, nodeAddress)
    leaderLatch = finalLeaderLatch

    finalLeaderLatch.addListener(new LeaderLatchListener() {
      override def isLeader(): Unit = {
        logger.info(finalLeaderLatch.getId + ":I am leader.")
        clusterService.setDataVersion(getLocalDataVersion())
        clusterService.setLeaderNodeAddress(nodeAddress)
      }
      override def notLeader(): Unit = {
        logger.info(finalLeaderLatch.getId + ":I am not leader.")
      }
    })
    finalLeaderLatch.start()
  }
}
