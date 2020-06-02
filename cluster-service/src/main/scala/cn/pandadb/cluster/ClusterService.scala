package cn.pandadb.cluster

import scala.collection.mutable.ListBuffer
import cn.pandadb.configuration.Config
import cn.pandadb.server.modules.LifecycleServerModule
import cn.pandadb.zk.ZKTools
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.{PathChildrenCacheEvent, PathChildrenCacheListener}
import org.apache.curator.framework.recipes.leader.{LeaderLatch, LeaderLatchListener, Participant}
import org.apache.curator.framework.recipes.locks.{InterProcessMutex, InterProcessReadWriteLock}
import org.apache.curator.shaded.com.google.common.net.HostAndPort
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
  val dataVersionLockPath = zkTools.buildFullZKPath("/lockPath")
  val versionZero = "0"
  var dataVersion: String = null
  var localDataVersion: String = null

  var leaderLatch : LeaderLatch = null
  val curator = zkTools.getCurator()
  var asFreshNodePath: String = null
  var asDataNodePath: String = null
  var asLeaderNodePath: String = null

  val roleEventlisteners = ListBuffer[NodeRoleChangedEventListener]()
  var lock: InterProcessReadWriteLock = null
  var interMux: InterProcessMutex = null

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
  /*  logger.info(this.getClass + ": doNodeStart")
    registerAsFreshNode1()
    updateLocalDataToLatestVersion()
    unregisterAsFreshNode()
    registerAsDataNode()
    participateInLeaderElection()
    while (getLeaderNode() == null) {
      logger.info("==== wait getLeaderNode ====")
      Thread.sleep(500)
    }*/
  }

  def lockDataVersion(isReadLock: Boolean): Unit = {

    if (lock == null) lock = new InterProcessReadWriteLock(curator, dataVersionLockPath)

    if (isReadLock) {
      interMux = lock.readLock()
    }
    else interMux = lock.writeLock()
    interMux.acquire()
  }

  def unLockDataVersion(): Unit = {

    if (interMux.isAcquiredInThisProcess()) interMux.release()

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

  def checkIsDataNode(node: String): Boolean = {
    if (zkTools.checkChildExist(dataNodesPath, node)) true
    else false
  }

  def registerPathChildrenListener(path: String, listenner: PathChildrenCacheListener): Unit = {
    zkTools.registerPathChildrenListener(path, listenner)
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
    //val dataNode = onLineNodePath + "/" + nodeAddress
    //zkTools.createZKNode(CreateMode.EPHEMERAL, dataNode)
    addNodeToPath(onLineNodePath, nodeAddress)
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

  def getLeaderNodeHostAndPort(): HostAndPort = {
    try {
      HostAndPort.fromString(getLeaderNode)
    } catch {
      case ex: NoNodeException => null
      case ex: Exception => throw ex
    }
  }

  def hasLeaderNode(): Boolean = {
    try {
      getLeaderLatch().getLeader.isLeader
    } catch {
      case ex: NoNodeException => null
      case ex: Exception => throw ex
    }
    false
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
    zkTools.assureZKNodeExist(leaderNodesPath, dataNodesPath, dataVersionPath, leaderLatchPath, dataVersionLockPath)
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
