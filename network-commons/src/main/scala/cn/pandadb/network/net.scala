package cn.pandadb.network

/**
  * Created by bluejoe on 2019/11/21.
  */
case class NodeAddress(host: String, port: Int) {
}

object NodeAddress {
  def fromString(url: String, seperator: String = ":"): NodeAddress = {
    val pair = url.split(seperator)
    NodeAddress(pair(0), pair(1).toInt)
  }
}

trait ClusterManager {
  def getWriteMasterNode(): NodeAddress;

  def getReadNode(): NodeAddress;

  def getCurrentState(): ClusterState;

  def getAllNodes(): NodeState;

  def changeState(state: ClusterState): Unit;
}

case class NodeState(address: NodeAddress, writable: Boolean, readable: Boolean) {

}

trait ClusterState {

}

abstract class ZookeeperBasedClusterManager(zkString: String) extends ClusterManager {

}