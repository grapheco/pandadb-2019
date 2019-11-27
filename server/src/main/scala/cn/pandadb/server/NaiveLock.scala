package cn.pandadb.server

import cn.pandadb.network.{NodeAddress, ZookeerperBasedClusterClient}

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created in 23:28 2019/11/27
  * @Modified By:
  */
trait NaiveLock {
  // acquire
  def lock()

  // release
  def unlock()


}

class NaiveWriteLock(allNodes: Iterable[NodeAddress], clusterClient: ZookeerperBasedClusterClient) extends NaiveLock {

  val nodeList = allNodes.toList
  var masterNodeAddress: NodeAddress = _
  val register = new ZKServiceRegistry(clusterClient.zkServerAddress)

  override def lock(): Unit = {
    nodeList.foreach(lockOrdinaryNode(_))
    lockLeaderNode(masterNodeAddress)
  }
  override def unlock(): Unit = {
    nodeList.foreach(unlockOrdinaryNode(_))
    unlockLeaderNode(masterNodeAddress)
  }

  def lockOrdinaryNode(node: NodeAddress): Unit = {
    register.unRegisterOrdinaryNode(node)
  }

  def lockLeaderNode(node: NodeAddress): Unit = {
    register.unRegisterLeaderNode(node)
  }

  def unlockOrdinaryNode(node: NodeAddress): Unit = {
    register.registerAsOrdinaryNode(node)
  }
  def unlockLeaderNode(node: NodeAddress): Unit = {
    register.registerAsLeader(node)
  }
}

class NaiveReadLock(allNodes: Iterable[NodeAddress], clusterClient: ZookeerperBasedClusterClient) extends NaiveLock {

  val nodeList = allNodes.toList
  val register = new ZKServiceRegistry(clusterClient.zkServerAddress)

  override def lock(): Unit = {
    nodeList.foreach(lockOrdinaryNode(_))
  }

  override def unlock(): Unit = {
    nodeList.foreach(unlockOrdinaryNode(_))
  }

  def lockOrdinaryNode(node: NodeAddress): Unit = {
    register.unRegisterOrdinaryNode(node)
  }
  def unlockOrdinaryNode(node: NodeAddress): Unit = {
    register.registerAsOrdinaryNode(node)
  }
}
