package cn.pandadb.server

import cn.pandadb.network._

class ZKClusterEventListener() extends ClusterEventListener {
  override def onEvent(event: ClusterEvent): Unit = {
    event match {
        // Not implemented.
      case ClusterStateChanged() => _;
      case NodeConnected(nodeAddress) => _;
      case NodeConnected(nodeAddress) => _;
      case ReadRequestAccepted() => _;
      case WriteRequestAccepted() => _;
      case ReadRequestCompleted() => _;
      case WriteRequestCompleted() => _;
      case MasterWriteNodeSeleted() => _;
      case READY_TO_WRITE() => _;
      case WRITE_FINISHED() => _;

     }
  }
}
