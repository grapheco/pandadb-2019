package cn.pandadb.server

import cn.pandadb.network.{ClusterClient, ClusterState, Finished, NodeAddress, UnlockedServing, Writing, ZKClusterEventListener, ZKPathConfig, ZookeerperBasedClusterClient}
import org.apache.curator.framework.CuratorFramework
import org.neo4j.driver.{AuthToken, AuthTokens, GraphDatabase}

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

  // async or sync?
  //def distributeWriteStatement(cypher: String)
}

class MasterRole(zkClusterClient: ZookeerperBasedClusterClient) extends Master {

  override var listenerList: List[ZKClusterEventListener] = _
  // how to init it?
  private var currentState: ClusterState = new ClusterState {}

  override val clusterClient = zkClusterClient
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
    for (nodeAddress <- allNodes) {
      val driver = GraphDatabase.driver(nodeAddress.getAsStr(),
        AuthTokens.basic("", ""))
      val session = driver.session()
      val tx = session.beginTransaction()
      tx.run(cypher)
      tx.success()
      session.close()
    }
  }

  override def clusterWrite(cypher: String): Unit = {
    initWriteContext()
    setClusterState(new Writing)
    globalWriteLock.lock()
    distributeWriteStatement(cypher)
    globalWriteLock.unlock()
    setClusterState(new Finished)
    setClusterState(new UnlockedServing)
  }

  override def addListener(listener: ZKClusterEventListener): Unit = {
    listenerList = listener :: listenerList
  }

}




