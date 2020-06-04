package cn.pandadb.server

import cn.pandadb.cluster.ClusterService
import cn.pandadb.configuration.Config
import cn.pandadb.leadernode.LeaderNodeDriver
import cn.pandadb.store.local.DataStore
import cn.pandadb.server.modules.LifecycleServerModule
import cn.pandadb.util.CompressDbFileUtil
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
    if(!isLeaderNode) {
      logger.info(this.getClass + ": not leaderNode: " + nodeHostAndPort)
      if (inElection) {
        if (clusterService.leaderLatch != null) leaderLatch.close()
        clusterService.leaderLatch = null
        inElection = false
      }
      tryToBecomeDataNode()
    }
  }

  def getLeaderLatch(): LeaderLatch = {
    if (leaderLatch == null) {
      leaderLatch = new LeaderLatch(clusterService.curator, clusterService.leaderLatchPath, nodeAddress)
    }
    leaderLatch
  }

  def isLeaderNode(): Boolean = {
    getLeaderLatch().hasLeadership()
  }
  def getLocalDataVersion(): String = {
    dataStore.getDataVersion().toString
  }

  def registerListenner(): Unit = {
    //listen leaderNode
    val leaderNodeCacheListener = new PathChildrenCacheListener {
      override def childEvent(curatorFramework: CuratorFramework, pathChildrenCacheEvent: PathChildrenCacheEvent): Unit = {
        val eventType = pathChildrenCacheEvent.getType
        eventType match {
          case PathChildrenCacheEvent.Type.CHILD_REMOVED => {
            if (!isLeaderNode()) {
              assureLeaderExist()
              tryToBecomeDataNode()
            }
          }
          case _ => null
        }
      }
    }
    clusterService.registerPathChildrenListener(clusterService.leaderNodesPath, leaderNodeCacheListener)

    //listen dataNode
    val dataNodeCacheListener = new PathChildrenCacheListener {
      override def childEvent(curatorFramework: CuratorFramework, pathChildrenCacheEvent: PathChildrenCacheEvent): Unit = {
        val eventType = pathChildrenCacheEvent.getType
        eventType match {
          case PathChildrenCacheEvent.Type.CHILD_REMOVED => {
            if (isLeaderNode()) {
              val data = pathChildrenCacheEvent.getData.getPath.split("/").last
              logger.info(this.getClass + ": dataNode shutdown: " + data)
            }
          }
          case PathChildrenCacheEvent.Type.CHILD_ADDED => {
            if (isLeaderNode()) {
              val data = pathChildrenCacheEvent.getData.getPath.split("/").last
              logger.info(this.getClass + ": dataNode online: " + data)
            }
          }
          case _ => null
        }
      }
    }
    clusterService.registerPathChildrenListener(clusterService.dataNodesPath, dataNodeCacheListener)
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
      clusterService.lockDataVersion(true)
      dataVersion = clusterService.getDataVersion()
      localDataVersion = getLocalDataVersion()
      if (dataVersion.toInt > localDataVersion.toInt) {
        syncDataFromCluster(HostAndPort.fromString(clusterService.getLeaderNodeAddress()))
      }
      clusterService.registerAsDataNode(nodeAddress)
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
  }
  def syncDataFromCluster(leaderNode: HostAndPort): Unit = {
    logger.info("syncDataFromCluster")
    val leaderDriver = new LeaderNodeDriver
    val clientConfig = RpcEnvClientConfig(new RpcConf(), config.getRpcServerName())
    val clientRpcEnv = HippoRpcEnvFactory.create(clientConfig)

    val dbDir = dataStore.graphStoreDirectory
    val downloadZipPath = dbDir
    val zipFileName = "DB_FILES.zip"
    val decompressTo = dbDir.replace("graph.db", "")
    val ref = leaderDriver.pullCompressedDbFileFromDataNode(
      downloadZipPath, zipFileName, clientRpcEnv, clusterService, config
    )
    clientRpcEnv.stop(ref)
    // deCompress
    val compressUtil = new CompressDbFileUtil
    compressUtil.decompress(downloadZipPath + zipFileName, decompressTo)

    logger.info(s"syncDataFromCluster: pull data <fromVersion: ${dataStore.getDataVersion()}> to dir <$dbDir>")
    logger.info(s"decompressTo: ${decompressTo}")
    logger.info("update local data version")
    //dataStore.setDataVersion(0)
  }

  def participateInLeaderElection(): Unit = {
    logger.info(this.getClass + "participateInLeaderElection: " + nodeAddress)
    val finalLeaderLatch = new LeaderLatch(clusterService.curator, clusterService.leaderLatchPath, nodeAddress)
    leaderLatch = finalLeaderLatch

    finalLeaderLatch.addListener(new LeaderLatchListener() {
      override def isLeader(): Unit = {
        logger.info(finalLeaderLatch.getId + ":I am leader.")
        clusterService.lockDataVersion(false)
        clusterService.setDataVersion(getLocalDataVersion())
        if (!clusterService.checkIsDataNode(nodeAddress)) clusterService.registerAsDataNode(nodeAddress)
        clusterService.unLockDataVersion()
        clusterService.setLeaderNodeAddress(nodeAddress)
      }
      override def notLeader(): Unit = {
        logger.info(finalLeaderLatch.getId + ":I am not leader.")
      }
    })
    finalLeaderLatch.start()
  }
}
