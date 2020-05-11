package cn.pandadb.cluster

import scala.collection.mutable.ListBuffer
import cn.pandadb.configuration.Config
import cn.pandadb.server.modules.LifecycleServerModule
import cn.pandadb.zk.ZKTools
import org.apache.curator.framework.recipes.leader.{LeaderLatch, LeaderLatchListener, Participant}
import org.apache.zookeeper.KeeperException.NoNodeException
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

  var leaderLatch: LeaderLatch = null

  val roleEventlisteners = ListBuffer[NodeRoleChangedEventListener]()

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
    updateLocalDataToLatestVersion()
    unregisterAsFreshNode()
    registerAsDataNode()
    participateInLeaderElection()
    while (getLeaderNode() == null) {
      logger.info("==== wait getLeaderNode ====")
      Thread.sleep(500)
    }
  }

  def updateLocalDataToLatestVersion(): Unit = {
    logger.info(this.getClass + ": syncLocalGraphData")
    val leaderNodeAddress = getLeaderNode()
    logger.info("LeaderNode: " + leaderNodeAddress)
    logger.info("pull ")
  }

  def registerAsFreshNode(): Unit = {
    logger.info(this.getClass + ": registerAsFreshNodes: " + nodeAddress)
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

//  def registerAsLeaderNode(): Unit = {
//    logger.info(this.getClass + "registerAsLeaderNode: " + nodeAddress)
//    val leaderNodePrefix = leaderNodesPath + "/" + "node-"
//    asLeaderNodePath = zkTools.createZKNode(CreateMode.EPHEMERAL_SEQUENTIAL, leaderNodePrefix, nodeAddress)
//  }
//
//  def unregisterAsLeaderNode: Unit = {
//    logger.info(this.getClass + "unregisterAsLeaderNode: " + nodeAddress)
//    zkTools.deleteZKNodeAndChildren(asLeaderNodePath)
//    asLeaderNodePath = null
//  }

  def participateInLeaderElection(): Unit = {
    logger.info(this.getClass + "participateInLeaderElection: " + nodeAddress)
    val finalLeaderLatch = new LeaderLatch(curator, leaderNodesPath, nodeAddress)
    leaderLatch = finalLeaderLatch

    finalLeaderLatch.addListener(new LeaderLatchListener() {
      override def isLeader(): Unit = {
        logger.info(finalLeaderLatch.getId + ":I am leader.")
        changeNodeRole(new LeaderNodeChangedEvent(true, getLeaderNode()))
      }
      override def notLeader(): Unit = {
        logger.info(finalLeaderLatch.getId + ":I am not leader.")
        changeNodeRole(new LeaderNodeChangedEvent(false, getLeaderNode()))
      }
    })
    finalLeaderLatch.start()
  }

  def getLeaderLatch(): LeaderLatch = {
    if (leaderLatch == null) {
      leaderLatch = new LeaderLatch(curator, leaderNodesPath)
    }
    leaderLatch
  }

  def getLeaderNode(): String = {
    try {
      getLeaderLatch().getLeader.getId
    } catch {
      case ex: NoNodeException => null
      case ex: Exception => throw ex
    }
  }

  def isLeaderNode(): Boolean = {
    getLeaderLatch().hasLeadership()
  }

  def getDataNodes(): List[String] = {
    val dataNodes = zkTools.getZKNodeChildren(dataNodesPath)
    dataNodes.map(name => {
      zkTools.getZKNodeData(dataNodesPath + "/" + name)
    })
  }

  def setDataVersion(version: String): Unit = {
    zkTools.createZKNode(CreateMode.PERSISTENT, dataVersionPath, version)
  }

  private def assurePathExist(): Unit = {
    zkTools.assureZKNodeExist(leaderNodesPath, dataNodesPath, dataVersionPath, freshNodesPath)
  }

  def addNodeRoleChangedEventListener(listener: NodeRoleChangedEventListener): Unit = {
    logger.info(this.getClass + ": addNodeRoleChangedEventListener" + ":" + config.getNodeAddress())
    roleEventlisteners.append(listener)
  }

  def changeNodeRole(event: NodeRoleChangedEvent): Unit = {
    logger.info(this.getClass + ": changeNodeRole" + ": " + roleEventlisteners.size.toString)
    roleEventlisteners.foreach(listener => listener.notifyRoleChanged(event))
  }
}
