package cn.pandadb.server

import cn.pandadb.network._
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.{CreateMode, ZooDefs}
import org.neo4j.driver._

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * @Author: Airzihao
  * @Description: This class is instanced when a node is selected as master node.
  * @Date: Created at 13:13 2019/11/27
  * @Modified By:
  */

trait Master {

  // get from zkBasedClusterClient
  var allNodes: Iterable[NodeAddress]

  //zkBasedClusterClient
  val clusterClient: ClusterClient

  // delay all write/read requests, implements by curator
  var globalWriteLock: NaiveLock //:curator lock

  // delay write requests only, implements by curator
  var globalReadLock: NaiveLock //:curator lock

  // inform these listeners the cluster context change?
  var listenerList: List[ZKClusterEventListener]

  def addListener(listener: ZKClusterEventListener)

  def clusterWrite(cypher: String)

}

class MasterRole(zkClusterClient: ZookeeperBasedClusterClient, localAddress: NodeAddress) extends Master {

  val localNodeAddress = localAddress
  override var listenerList: List[ZKClusterEventListener] = _

  // how to init it?
  private var currentState: ClusterState = new ClusterState {}
  override val clusterClient = zkClusterClient
  val masterNodeAddress = clusterClient.getWriteMasterNode("").get.getAsStr()
  override var allNodes: Iterable[NodeAddress] = clusterClient.getAllNodes()
  override var globalReadLock: NaiveLock = new NaiveReadLock(allNodes, clusterClient)
  override var globalWriteLock: NaiveLock = new NaiveWriteLock(allNodes, clusterClient)

  private def initWriteContext(): Unit = {
    allNodes = clusterClient.getAllNodes()
    globalReadLock = new NaiveWriteLock(allNodes, clusterClient)
    globalWriteLock = new NaiveWriteLock(allNodes, clusterClient)
  }

  def setClusterState(state: ClusterState): Unit = {
    currentState = state
  }

  private def distributeWriteStatement(cypher: String): Unit = {

    var tempResult: StatementResult = null
    var futureTasks = new ListBuffer[Future[Boolean]]
    for (nodeAddress <- allNodes) {
      if (nodeAddress.getAsStr() != masterNodeAddress) {
        val future = Future[Boolean] {
          try {
            val uri = s"bolt://" + nodeAddress.getAsStr()
            val driver = GraphDatabase.driver(uri,
              AuthTokens.basic("", ""))
            val session = driver.session()
            val tx = session.beginTransaction()
            tempResult = tx.run(cypher)
            tx.success()
            tx.close()
            session.close()
            true
          } catch {
            case e: Exception =>
              throw new Exception("Write-cluster operation failed.")
              false
          }
        }
        futureTasks.append(future)
      }
    }
    futureTasks.foreach(future => Await.result(future, 3.seconds))
  }

  // TODO finetune the state change mechanism
  override def clusterWrite(cypher: String): Unit = {

    val preVersion = zkClusterClient.getClusterDataVersion()
    initWriteContext()
    setClusterState(new Writing)
    globalWriteLock.lock()

    // key func
    distributeWriteStatement(cypher)

    globalWriteLock.unlock()
    setClusterState(new Finished)
    setClusterState(new UnlockedServing)

    // had better put these operations to FINISH state
    val curVersion = preVersion + 1
    _setDataVersion(curVersion)
  }

  def clusterRead(cypher: String): StatementResult = {
    val iter = allNodes.iterator
    var statementResult: StatementResult = null;
    while (iter.hasNext) {
      val str = iter.next().getAsStr()
      if( str != masterNodeAddress) {
        val uri = s"bolt://" + str
        val driver = GraphDatabase.driver(uri)
        statementResult = driver.session().run(cypher)
      }
    }
    statementResult
  }

  override def addListener(listener: ZKClusterEventListener): Unit = {
    listenerList = listener :: listenerList
  }

  private def _setDataVersion(curVersion: Int): Unit = {
    _updateFreshNode()
    clusterClient.curator.setData().forPath(ZKPathConfig.dataVersionPath, BytesTransform.serialize(curVersion))
  }

  private def _updateFreshNode(): Unit = {

    val children = clusterClient.curator.getChildren.forPath(ZKPathConfig.freshNodePath)
    // delete old node
    if(children.isEmpty == false) {
      val child = children.iterator()
      while (child.hasNext) {
        val fullPath = ZKPathConfig.freshNodePath + "/" + child.next()
        clusterClient.curator.delete().forPath(fullPath)
      }
    }

    val curFreshNodeRpc = PNodeServerContext.getLocalIpAddress + ":" + PNodeServerContext.getRpcPort.toString
    clusterClient.curator.create().creatingParentsIfNeeded()
      .withMode(CreateMode.PERSISTENT)
      .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
      .forPath(ZKPathConfig.freshNodePath + s"/" + curFreshNodeRpc)
  }

}

// todo: use this class to do multi threads write operation.
case class DriverWriteThread(driver: Driver, cypher: String) extends Thread {

  override def run(): Unit = {
    val session = driver.session()
    val tx = session.beginTransaction()
    try {
      tx.run(cypher)
      tx.success()
      tx.close()
      session.close()
    } catch {
      case e: Exception =>
        throw new Exception("Write cluster operation failed.")
    }
  }
}
