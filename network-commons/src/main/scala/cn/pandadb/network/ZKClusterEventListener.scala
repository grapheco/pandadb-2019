package cn.pandadb.network

class ZKClusterEventListener() extends ClusterEventListener {
  override def onEvent(event: ClusterEvent): Unit = {
    event match {
        // Not implemented.
      case ClusterStateChanged() => ;
      case NodeConnected(nodeAddress) => ;
      case NodeDisconnected(nodeAddress) => ;
      case ReadRequestAccepted() => ;
      case WriteRequestAccepted() => ;
      case ReadRequestCompleted() => ;
      case WriteRequestCompleted() => ;
      case MasterWriteNodeSeleted() => ;
      case READY_TO_WRITE() => ;
      case WRITE_FINISHED() => ;

     }
  }
}
