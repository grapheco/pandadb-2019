package cn.pandadb.server

import cn.pandadb.network.{NodeAddress, ZookeeperBasedClusterClient}

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created in 23:28 2019/11/27
  * @Modified By:
  */
trait NaiveLock {

  def lock()
  def unlock()
}

class NaiveWriteLock(clusterClient: ZookeeperBasedClusterClient) extends NaiveLock {

  val allNodes = clusterClient.getAllNodes()
  var nodeList = allNodes.toList
  val masterNodeAddress: NodeAddress = clusterClient.getWriteMasterNode("").get
  val register = new ZKServiceRegistry(clusterClient.zkServerAddress)

  override def lock(): Unit = {
    nodeList = clusterClient.getAllNodes().toList
    while (nodeList.length == 0) {
      Thread.sleep(1000)
    }
    nodeList.foreach(lockOrdinaryNode(_))
    lockLeaderNode(masterNodeAddress)
  }

  override def unlock(): Unit = {
    nodeList = clusterClient.getAllNodes().toList
    while (nodeList.length == 0) {
      Thread.sleep(1000)
    }
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

class NaiveReadLock(clusterClient: ZookeeperBasedClusterClient) extends NaiveLock {

  val register = new ZKServiceRegistry(clusterClient.zkServerAddress)
  var masterNodeAddress: NodeAddress = _

  override def lock(): Unit = {
    masterNodeAddress = clusterClient.getWriteMasterNode("").get
    lockLeaderNode(masterNodeAddress)
  }

  override def unlock(): Unit = {
    unlockLeaderNode(masterNodeAddress)
  }

  def lockLeaderNode(node: NodeAddress): Unit = {
    register.unRegisterLeaderNode(node)
  }
  def unlockLeaderNode(node: NodeAddress): Unit = {
    register.registerAsLeader(node)
  }
}
