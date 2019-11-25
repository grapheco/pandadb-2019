package cn.pandadb.network

trait ClusterEvent {

}

// Don't need to implement them, just for pattern match.
case class ClusterStateChanged() extends ClusterEvent {

}

case class NodeConnected(nodeAddress: NodeAddress) extends ClusterEvent {

}

case class NodeDisconnected(nodeAddress: NodeAddress) extends ClusterEvent {

}

case class ReadRequestAccepted() extends ClusterEvent {

}

case class WriteRequestAccepted() extends ClusterEvent {

}

case class ReadRequestCompleted() extends ClusterEvent {

}

case class WriteRequestCompleted() extends ClusterEvent {

}

case class MasterWriteNodeSeleted() extends ClusterEvent {

}

case class READY_TO_WRITE() extends ClusterEvent {

}

case class WRITE_FINISHED() extends ClusterEvent {

}