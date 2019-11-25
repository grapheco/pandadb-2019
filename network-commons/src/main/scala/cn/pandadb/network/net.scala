package cn.pandadb.network

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry

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

  def getAllNodes(): Iterable[NodeAddress];

  def getCurrentState(): ClusterState;

  def waitFor(state: ClusterState): Unit;

  def listen(listener: ClusterEventListener): Unit;
}

trait ClusterEventListener {
  def onEvent(event: ClusterEvent): Unit;
}

trait ClusterState {

}

case class LockedServing() extends ClusterState{

}

case class UnlockedServing() extends ClusterState{

}

case class PreWrite() extends ClusterState{
  // prepare to write, ignore all new requests.
}

case class Writing() extends ClusterState{

}

case class Finished() extends ClusterState{

}



//abstract class ZookeeperBasedClusterManager(zkString: String) extends ClusterClient {
//  //use Apache Curator
//  val curator: CuratorFramework = CuratorFrameworkFactory.newClient(zkString, new ExponentialBackoffRetry(1000, 3));
//  curator.start()
//}

class ZookeerperBasedClusterManager(zkConstants: ZKConstants) extends ClusterClient {

  val curator: CuratorFramework = CuratorFrameworkFactory.newClient(zkConstants.zkServerAddress, new ExponentialBackoffRetry(1000, 3));
  curator.start()
  private var currentState: ClusterState = _
  val registryPath = zkConstants.registryPath
  val leaderPath = zkConstants.leaderNodePath
  val ordinaryPath = zkConstants.ordinaryNodesPath

  override def getWriteMasterNode(): NodeAddress = {
    val leaderAddress = curator.getChildren().forPath(leaderPath).toString
    NodeAddress.fromString(leaderAddress)
  }

  override def getAllNodes(): Iterable[NodeAddress] = {
    val ordinaryNodes = curator.getChildren.forPath(ordinaryPath).iterator()
    var nodeAddresses: List[NodeAddress] = Nil
    while (ordinaryNodes.hasNext) {
      nodeAddresses = nodeAddresses :+ NodeAddress.fromString(ordinaryNodes.next())
    }
    val allNodes = nodeAddresses;
    allNodes
  }


  override def getCurrentState(): ClusterState = {
    currentState
  }

  override def listen(listener: ClusterEventListener): Unit = ???

  override def waitFor(state: ClusterState): Unit = null
}