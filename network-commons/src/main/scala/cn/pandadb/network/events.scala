package cn.pandadb.network

trait ClusterEvent {

}

case class ClusterStateChanged() extends ClusterEvent {

}

case class NodeConnected() extends ClusterEvent {

}

case class NodeDisconnected() extends ClusterEvent {

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