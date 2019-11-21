package cn.pandadb.cnode


trait ClusterStateEvent{

}

case class ReadRequest() extends ClusterStateEvent {

}

case class WriteRequest() extends ClusterStateEvent {

}

case class READY_TO_WRITE() extends ClusterStateEvent {

}

case class WRITE_FINISHED() extends ClusterStateEvent {

}

case class WAIT_JOIN() extends ClusterStateEvent {

}