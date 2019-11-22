package cn.pandadb.network

/**
  * Created by bluejoe on 2019/11/21.
  */
case class NodeAddress(host: String, port: Int) {
}

object NodeAddress {
  def fromString(url: String, separator: String = ":"): NodeAddress = {
    val pair = url.split(separator)
    NodeAddress(pair(0), pair(1).toInt)
  }
}

// used by server & driver
trait ClusterClient {
  def getWriteMasterNode(): NodeAddress;

  def getReadNode(): NodeAddress;

  def getCurrentState(): ClusterState;

  def getAllNodes(): NodeDetail;

  def waitFor(state: ClusterState): Unit;

  def listen(listener: ClusterEventListener): Unit;
}

trait ClusterEventListener {
  def onEvent(event: ClusterEvent): Unit;
}

trait ClusterState {

}

abstract class ZookeeperBasedClusterManager(zkString: String) extends ClusterClient {
  //use Apache Curator
}