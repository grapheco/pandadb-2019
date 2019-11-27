package cn.pandadb.network

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry

/**
  * Created by bluejoe on 2019/11/21.
  */
case class NodeAddress(host: String, port: Int) {
  def getAsStr(): String = {
    host + s":" + port.toString
  }
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
  def onEvent(event: ClusterEvent)
//  def onEvent(event: ClusterEvent): Unit = {
//    event match {
//      // Not implemented.
//      case ClusterStateChanged() => _;
//      case NodeConnected(nodeAddress) => _;
//      case NodeConnected(nodeAddress) => _;
//      case ReadRequestAccepted() => _;
//      case WriteRequestAccepted() => _;
//      case ReadRequestCompleted() => _;
//      case WriteRequestCompleted() => _;
//      case MasterWriteNodeSeleted() => _;
//      case READY_TO_WRITE() => _;
//      case WRITE_FINISHED() => _;
//    }
//  }
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