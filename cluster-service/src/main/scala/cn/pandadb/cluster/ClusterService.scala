package cn.pandadb.cluster

import cn.pandadb.configuration.Config
import cn.pandadb.server.modules.LifecycleServerModule
import cn.pandadb.zk.ZKTools
import org.apache.zookeeper.{CreateMode, ZooDefs}

class ClusterService(config: Config, zkTools: ZKTools) extends LifecycleServerModule {

  val logger = config.getLogger(this.getClass)
  val nodeAddress = config.getNodeAddress()

  val leaderNodesPath = zkTools.buildFullZKPath("/leaderNodes")
  val dataNodesPath = zkTools.buildFullZKPath("/dataNodes")
  val dataVersionPath = zkTools.buildFullZKPath("/dataVersion")
  val freshNodesPath = zkTools.buildFullZKPath("/freshNodes")

  val curator = zkTools.getCurator()
  var asFreshNodePath: String = null
  var asDataNodePath: String = null
  var asLeaderNodePath: String = null

  override def init(): Unit = {
    logger.info(this.getClass + ": init")
    assurePathExist()
  }

  override def start(): Unit = {
    logger.info(this.getClass + ": start")
    registerAsFreshNode()
  }

  override def stop(): Unit = {
    logger.info(this.getClass + ": stop")
  }

  override def shutdown(): Unit = {
    logger.info(this.getClass + ": shutdown")
  }

  def registerAsFreshNode(): Unit = {
    logger.info(this.getClass + "registerAsFreshNodes: " + nodeAddress)
    val freshNodePrefix = freshNodesPath + "/" + "node-"
    asFreshNodePath = curator.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
      .forPath(freshNodePrefix, nodeAddress.getBytes("UTF-8"))
  }

  def unregisterAsFreshNode: Unit = {
    curator.delete().deletingChildrenIfNeeded().forPath(asFreshNodePath)
    asFreshNodePath = null
  }

  def registerAsDataNode(): Unit = {
    val dataNodePrefix = dataNodesPath + "/" + "node-"
    asDataNodePath = curator.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
      .forPath(dataNodePrefix, nodeAddress.getBytes("UTF-8"))
  }

  def unregisterAsDataNode: Unit = {
    curator.delete().deletingChildrenIfNeeded().forPath(asDataNodePath)
    asDataNodePath = null
  }

  def registerAsLeaderNode(): Unit = {
    val leaderNodePrefix = leaderNodesPath + "/" + "node-"
    curator.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
      .forPath(leaderNodePrefix, nodeAddress.getBytes("UTF-8"))
  }

  def unregisterAsLeaderNode: Unit = {
    curator.delete().deletingChildrenIfNeeded().forPath(asLeaderNodePath)
    asLeaderNodePath = null
  }

  def setDataVersion(version: String): Unit = {
    curator.create().withMode(CreateMode.PERSISTENT).forPath(dataVersionPath, version.getBytes("UTF-8"))
  }

  private def assurePathExist(): Unit = {
    val tmpList = List(leaderNodesPath, dataNodesPath, dataVersionPath, freshNodesPath)
    tmpList.foreach(path => {
      if (curator.checkExists().forPath(path) == null) {
        curator.create()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.PERSISTENT)
          .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
          .forPath(path)
      }
    })
  }

}
