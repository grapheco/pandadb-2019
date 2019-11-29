package cn.pandadb.server

import cn.pandadb.network.{ClusterClient, ClusterState, Finished, NodeAddress, UnlockedServing, Writing, ZKClusterEventListener, ZKPathConfig, ZookeerperBasedClusterClient}
import org.apache.curator.framework.CuratorFramework
import org.neo4j.driver.{AuthToken, AuthTokens, GraphDatabase, StatementResult}

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

  def clusterWrite(cypher: String): StatementResult

}

class MasterRole(zkClusterClient: ZookeerperBasedClusterClient) extends Master {

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

  private def distributeWriteStatement(cypher: String): StatementResult = {

    var tempResult: StatementResult = null
    for (nodeAddress <- allNodes) {
      if (nodeAddress.getAsStr() != masterNodeAddress) {
        val uri = s"bolt://" + nodeAddress.getAsStr()
        val driver = GraphDatabase.driver(uri,
          AuthTokens.basic("", ""))
        val session = driver.session()
        val tx = session.beginTransaction()
        tempResult = tx.run(cypher)
        tx.success()
        session.close()
      }
    }
    tempResult
  }

  // TODO finetune the state change mechanism
  override def clusterWrite(cypher: String): StatementResult = {
    initWriteContext()
    setClusterState(new Writing)
    globalWriteLock.lock()
    val tempResult = distributeWriteStatement(cypher)
    globalWriteLock.unlock()
    setClusterState(new Finished)
    setClusterState(new UnlockedServing)
    tempResult
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

}




