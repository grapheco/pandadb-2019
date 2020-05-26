package cn.pandadb.cluster

import scala.collection.mutable.ListBuffer
import cn.pandadb.configuration.Config
import cn.pandadb.server.modules.LifecycleServerModule
import cn.pandadb.zk.ZKTools
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.{PathChildrenCacheEvent, PathChildrenCacheListener}
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
  val leaderLatchPath = zkTools.buildFullZKPath("/leaderLatch")
  val onLineNodePath = zkTools.buildFullZKPath("/onLineNode")
  val versionZero = "0"

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

  def assureLeaderExist(): Unit = {

    var leaderNodeAddress = getLeaderNodeAddress()
    var dataVersion = getDataVersion()
    val localDataVersion = getLocalDataVersion()
    var inElection = false
    while (leaderNodeAddress == null) {
      logger.info(this.getClass + ": cluster has no leader")
      if (dataVersion.toInt <= localDataVersion.toInt && !inElection) {
        inElection = true
        participateInLeaderElection()
      }
      logger.info("==== wait getLeaderNode ====")
      Thread.sleep(500)
      leaderNodeAddress = getLeaderNodeAddress()
      dataVersion = getDataVersion()
    }
    logger.info(this.getClass + ": cluster has a new leader: " + leaderNodeAddress)
    logger.info(this.getClass + ": dataVersion: " + dataVersion)

  }
  //todo when leader is down,when datanode is done,when node want to become datanode
  def doNodeStart2(): Unit = {
    logger.info(this.getClass + ": doNodeStart")
    registerAsOnLineNode()
    assureLeaderExist()
    if(!isLeaderNode()) {
      val dataVersion = getDataVersion()
      val localDataVersion = getLocalDataVersion()
      while (dataVersion.toInt > localDataVersion.toInt) {
        updateLocalDataToLatestVersion()
      }
      //todo should init something for read and write data
      registerAsFreshNode()

      val leaderChildrenCacheListener = new PathChildrenCacheListener {
        override def childEvent(curatorFramework: CuratorFramework, pathChildrenCacheEvent: PathChildrenCacheEvent): Unit = {
          val eventType = pathChildrenCacheEvent.getType
          eventType match {
            case PathChildrenCacheEvent.Type.CHILD_REMOVED => assureLeaderExist()
            case _ => null
          }
        }
      }
      zkTools.registerPathChildrenListener(leaderNodesPath, leaderChildrenCacheListener)
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

  def registerAsOnLineNode(): Unit = {
    logger.info(this.getClass + "registerAsOnLineNode: " + nodeAddress)
    val dataNode = onLineNodePath + "/" + nodeAddress
    zkTools.createZKNode(CreateMode.EPHEMERAL_SEQUENTIAL, dataNode)
  }

  def unregisterAsDataNode(): Unit = {
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
    val finalLeaderLatch = new LeaderLatch(curator, leaderLatchPath, nodeAddress)
    leaderLatch = finalLeaderLatch

    finalLeaderLatch.addListener(new LeaderLatchListener() {
      override def isLeader(): Unit = {
        logger.info(finalLeaderLatch.getId + ":I am leader.")
        setDataVersion(getLocalDataVersion())
        setLeaderNodeAddress(nodeAddress)
        //changeNodeRole(new LeaderNodeChangedEvent(true, getLeaderNode()))
        val pathChildrenCacheListener = new PathChildrenCacheListener() {
          override def childEvent(curatorFramework: CuratorFramework, pathChildrenCacheEvent: PathChildrenCacheEvent): Unit = {
            val eventType = pathChildrenCacheEvent.getType()
            eventType match {
              case PathChildrenCacheEvent.Type.CHILD_ADDED => {
                //todo get lock of dataVersion
                val freshNodes = zkTools.getZKNodeChildren(freshNodesPath)
                zkTools.deleteZKNodeAndChildren(freshNodesPath)
                freshNodes.foreach(u => zkTools.createZKNode(CreateMode.EPHEMERAL, dataNodesPath, u))
              }
              case _ => null
            }
          }
        }
        zkTools.registerPathChildrenListener(freshNodesPath, pathChildrenCacheListener)
      }
      override def notLeader(): Unit = {
        logger.info(finalLeaderLatch.getId + ":I am not leader.")
        //changeNodeRole(new LeaderNodeChangedEvent(false, getLeaderNode()))
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

  def getDataVersion(): String = {
    val dataVersion = zkTools.getZKNodeChildren(dataVersionPath)
    if (dataVersion isEmpty) versionZero else dataVersion.head
  }

  def getLocalDataVersion(): String = {
    versionZero
  }

  def setDataVersion(version: String): Unit = {
    val path = dataVersionPath + "/" + version
    zkTools.createZKNode(CreateMode.PERSISTENT, path)
  }

  def getLeaderNodeAddress(): String = {
    val leaderNode = zkTools.getZKNodeChildren(leaderNodesPath)
    if (leaderNode isEmpty) null else leaderNode.head
  }
  def setLeaderNodeAddress(nodeAddress: String): Unit = {
    val path = leaderNodesPath + "/" + nodeAddress
    zkTools.createZKNode(CreateMode.EPHEMERAL, path)
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
