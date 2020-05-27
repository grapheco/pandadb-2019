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
  val unfreshNodesPath = zkTools.buildFullZKPath("/unFreshNodes")
  val versionZero = "0"
  var dataVersion: String = null
  var localDataVersion: String = null

  var inElection = false
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

  def lockDataVersion(): Unit = {
      //todo
  }

  def unLockDataVersion(): Unit = {
    //todo
  }

  def checkNodeOk(nodeWithVersion: String): Boolean = {
    val data = nodeWithVersion.split("_")
    if (data.last.toInt == getDataVersion().toInt) true
    else false
  }

  def deleteAllPathNode(path: String): Unit = {
    zkTools.deleteZKNodeAndChildren(path)
  }

  def addNodeToPath(path: String, node: String): Unit = {
    if (zkTools.checkChildExist(path, node)) zkTools.deleteZKNodeAndChildren(path + "/" + node)
    zkTools.createZKNode(CreateMode.EPHEMERAL, path + "/" + node)
  }

  def moveNodeFromFreshToData(): Unit = {
    val nodeList = zkTools.getZKNodeChildren(freshNodesPath)
    deleteAllPathNode(freshNodesPath)
    nodeList.foreach(u => if (checkNodeOk(u)) addNodeToPath(dataNodesPath, u.split("_").head)
    else addNodeToPath(unfreshNodesPath, u.split("_").head))
  }

  def tryToBecomeDataNode(): Unit = {
    //todo
    logger.info(this.getClass + ": tryToBecomeDataNode" + nodeAddress)
    var dataVersion = getDataVersion()
    var localDataVersion = getLocalDataVersion()
    while (dataVersion.toInt > localDataVersion.toInt) {
      updateLocalDataToLatestVersion()
      dataVersion = getDataVersion()
      localDataVersion = getLocalDataVersion()
    }
    //todo should init something for read and write data
    registerAsFreshNode()
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
    zkTools.registerPathChildrenListener(leaderNodesPath, leaderNodeCacheListener)

    //listen freshNode
    val freshNodeCacheListener = new PathChildrenCacheListener {
      override def childEvent(curatorFramework: CuratorFramework, pathChildrenCacheEvent: PathChildrenCacheEvent): Unit = {
        val eventType = pathChildrenCacheEvent.getType
        eventType match {
          case PathChildrenCacheEvent.Type.CHILD_ADDED => {
            if (isLeaderNode()) {
              lockDataVersion()
              moveNodeFromFreshToData()
              unLockDataVersion()
            }
          }
          case _ => null
        }
      }
    }
    zkTools.registerPathChildrenListener(freshNodesPath, freshNodeCacheListener)

    //listen OnlineNode
    val onLineNodeCacheListener = new PathChildrenCacheListener {
      override def childEvent(curatorFramework: CuratorFramework, pathChildrenCacheEvent: PathChildrenCacheEvent): Unit = {
        val eventType = pathChildrenCacheEvent.getType
        eventType match {
          case PathChildrenCacheEvent.Type.CHILD_REMOVED => {
            if (isLeaderNode()) {
              //todo check if dataNode off line
            }
          }
          case _ => null
        }
      }
    }
    zkTools.registerPathChildrenListener(onLineNodePath, onLineNodeCacheListener)

    //listen dataNode
    val dataNodeCacheListener = new PathChildrenCacheListener {
      override def childEvent(curatorFramework: CuratorFramework, pathChildrenCacheEvent: PathChildrenCacheEvent): Unit = {
        val eventType = pathChildrenCacheEvent.getType
        eventType match {
          case PathChildrenCacheEvent.Type.CHILD_ADDED => {
            if (!isLeaderNode()) {
              if (zkTools.checkChildExist(dataNodesPath, nodeAddress)&& !inElection) {
                logger.info(this.getClass + ": I'm datanode now" + nodeAddress)
                participateInLeaderElection()
                inElection = true
              }
            }
          }
          case _ => null
        }
      }
    }
    zkTools.registerPathChildrenListener(dataNodesPath, dataNodeCacheListener)

    val unFreshNodeCacheListener = new PathChildrenCacheListener {
      override def childEvent(curatorFramework: CuratorFramework, pathChildrenCacheEvent: PathChildrenCacheEvent): Unit = {
        val eventType = pathChildrenCacheEvent.getType
        eventType match {
          case PathChildrenCacheEvent.Type.CHILD_ADDED => {
            if (!isLeaderNode()) {
              if (zkTools.checkChildExist(dataNodesPath, nodeAddress)) {
                zkTools.deleteZKNodeAndChildren(unfreshNodesPath + "/" + nodeAddress)
                tryToBecomeDataNode()
              }
            }
          }
          case _ => null
        }
      }
    }
    zkTools.registerPathChildrenListener(unfreshNodesPath, unFreshNodeCacheListener)
  }
  def assureLeaderExist(): Unit = {

    var leaderNodeAddress = getLeaderNodeAddress()
    var dataVersion = getDataVersion()
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
      leaderNodeAddress = getLeaderNodeAddress()
      dataVersion = getDataVersion()
    }
    logger.info(this.getClass + ": cluster has a new leader: " + leaderNodeAddress)
    logger.info(this.getClass + ": dataVersion: " + dataVersion)

  }

  def doNodeStart2(): Unit = {
    logger.info(this.getClass + ": doNodeStart")
    registerListenner()
    registerAsOnLineNode()
    assureLeaderExist()
    if(!isLeaderNode()) {
      if (inElection) {
        if (leaderLatch != null) leaderLatch.close()
        leaderLatch = null
        inElection = false
      }
      tryToBecomeDataNode()
    }

  }

  def updateLocalDataToLatestVersion(): Unit = {
    logger.info(this.getClass + ": syncLocalGraphData")
    logger.info(this.getClass + ": dataVersion = " + dataVersion)
    logger.info(this.getClass + ": localdataVersion = " + localDataVersion)
    val leaderNodeAddress = getLeaderNodeAddress()
    logger.info("LeaderNode: " + leaderNodeAddress)
    logger.info("pull ")
    localDataVersion = (localDataVersion.toInt + 1 ).toString
  }

  def registerAsFreshNode1(): Unit = {
    logger.info(this.getClass + ": registerAsFreshNodes: " + nodeAddress)
    val freshNodePrefix = freshNodesPath + "/" + "node-"
    asFreshNodePath = zkTools.createZKNode(CreateMode.EPHEMERAL_SEQUENTIAL, freshNodePrefix, nodeAddress)
  }

  def registerAsFreshNode(): Unit = {
    logger.info(this.getClass + ": registerAsFreshNodes: " + nodeAddress)
    val freshNode = freshNodesPath + "/" + nodeAddress + "_" + getLocalDataVersion()
    asFreshNodePath = zkTools.createZKNode(CreateMode.EPHEMERAL, freshNode)
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
    if(!zkTools.checkPathExist(leaderLatchPath)) zkTools.createZKNode(CreateMode.PERSISTENT, leaderLatchPath)
    val finalLeaderLatch = new LeaderLatch(curator, leaderLatchPath, nodeAddress)
    leaderLatch = finalLeaderLatch

    finalLeaderLatch.addListener(new LeaderLatchListener() {
      override def isLeader(): Unit = {
        logger.info(finalLeaderLatch.getId + ":I am leader.")
        //if (getDataVersion().toInt > getLocalDataVersion().toInt) leaderLatch.close()
        setDataVersion(getLocalDataVersion())
        setLeaderNodeAddress(nodeAddress)
        //zkTools.deleteZKNodeAndChildren(leaderLatchPath)
        //changeNodeRole(new LeaderNodeChangedEvent(true, getLeaderNode()))

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
    //val dataVersion = zkTools.getZKNodeChildren(dataVersionPath)
    //if (dataVersion isEmpty) versionZero else dataVersion.head
    dataVersion
  }

  def getLocalDataVersion(): String = {
    //versionZero
    localDataVersion
  }

  def setDataVersion(version: String): Unit = {
    val path = dataVersionPath + "/" + version
    if (!zkTools.checkChildExist(dataVersionPath, version)) zkTools.createZKNode(CreateMode.PERSISTENT, path)
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
    zkTools.assureZKNodeExist(leaderNodesPath, dataNodesPath, dataVersionPath, freshNodesPath, onLineNodePath,
      leaderLatchPath, unfreshNodesPath)
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
