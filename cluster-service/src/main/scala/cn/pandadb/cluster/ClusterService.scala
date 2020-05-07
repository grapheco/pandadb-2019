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

//  val curator = zkTools.getCurator()
  var asFreshNodePath: String = null
  var asDataNodePath: String = null
  var asLeaderNodePath: String = null

  override def init(): Unit = {
    logger.info(this.getClass + ": init")
    assurePathExist()
  }

  override def start(): Unit = {
    logger.info(this.getClass + ": start")
    doNodeStart()
  }

  override def stop(): Unit = {
    logger.info(this.getClass + ": stop")
  }

  override def shutdown(): Unit = {
    logger.info(this.getClass + ": shutdown")
  }

  def doNodeStart(): Unit = {
    logger.info(this.getClass + ": doNodeStart")
    registerAsFreshNode()
    syncLocalGraphData()
    unregisterAsFreshNode()
    registerAsDataNode()
  }

  def syncLocalGraphData(): Unit = {
    logger.info(this.getClass + ": syncLocalGraphData")
    val leaderNodeAddress = getLeaderNode()
    logger.info("LeaderNode: " + leaderNodeAddress)
    logger.info("pull ")
  }

  def registerAsFreshNode(): Unit = {
    logger.info(this.getClass + "registerAsFreshNodes: " + nodeAddress)
    val freshNodePrefix = freshNodesPath + "/" + "node-"
    asFreshNodePath = zkTools.createZKNode(CreateMode.EPHEMERAL_SEQUENTIAL, freshNodePrefix, nodeAddress)
  }

  def unregisterAsFreshNode(): Unit = {
    logger.info(this.getClass + "unregisterAsFreshNode: " + nodeAddress)
    zkTools.deleteZKNodeAndChildren(asFreshNodePath)
    asFreshNodePath = null
  }

  def registerAsDataNode(): Unit = {
    logger.info(this.getClass + "registerAsDataNode: " + nodeAddress)
    val dataNodePrefix = dataNodesPath + "/" + "node-"
    asDataNodePath = zkTools.createZKNode(CreateMode.EPHEMERAL_SEQUENTIAL, dataNodePrefix, nodeAddress)

  }

  def unregisterAsDataNode: Unit = {
    logger.info(this.getClass + "unregisterAsDataNode: " + nodeAddress)
    zkTools.deleteZKNodeAndChildren(asDataNodePath)
    asDataNodePath = null
  }

  def registerAsLeaderNode(): Unit = {
    logger.info(this.getClass + "registerAsLeaderNode: " + nodeAddress)
    val leaderNodePrefix = leaderNodesPath + "/" + "node-"
    asLeaderNodePath = zkTools.createZKNode(CreateMode.EPHEMERAL_SEQUENTIAL, leaderNodePrefix, nodeAddress)
  }

  def unregisterAsLeaderNode: Unit = {
    logger.info(this.getClass + "unregisterAsLeaderNode: " + nodeAddress)
    zkTools.deleteZKNodeAndChildren(asLeaderNodePath)
    asLeaderNodePath = null
  }

  def getLeaderNode(): String = {
    val leaderNodes = zkTools.getZKNodeChildren(leaderNodesPath)
    if (leaderNodes.length > 0) {
      leaderNodes(0)
    }
    null
  }

  def getDataNodes(): List[String] = {
    zkTools.getZKNodeChildren(dataNodesPath)
  }

  def setDataVersion(version: String): Unit = {
    zkTools.createZKNode(CreateMode.PERSISTENT, dataVersionPath, version)
  }

  private def assurePathExist(): Unit = {
    zkTools.assureZKNodeExist(leaderNodesPath, dataNodesPath, dataVersionPath, freshNodesPath)
  }

}
