package cn.pandadb.cnode




class ClusterStateContext {

  private var curState:ClusterState = _

  var dataVersion:String = _

  // freely set???
  def setClusterState(state:ClusterState): Unit = {
    curState = state
  }
  def getClusterState() = curState

  // how to implement
  def request(event: ClusterStateEvent): Unit ={

  }
}


class ClusterStateMachine {

}

trait ClusterState{
  def handle(context: ClusterStateContext, event: ClusterStateEvent)
}
case class UnlockedServing(clusterStateContext: ClusterStateContext) extends ClusterState {
  override def handle(context: ClusterStateContext, event: ClusterStateEvent): Unit = ???
}
case class LockedServing(clusterStateContext: ClusterStateContext) extends ClusterState {
  override def handle(context: ClusterStateContext, event: ClusterStateEvent): Unit = ???
}
case class PreWrite(clusterStateContext: ClusterStateContext) extends ClusterState {
  override def handle(context: ClusterStateContext, event: ClusterStateEvent): Unit = ???
}
case class Write(clusterStateContext: ClusterStateContext) extends ClusterState {
  override def handle(context: ClusterStateContext, event: ClusterStateEvent): Unit = ???
}
case class Finish(clusterStateContext: ClusterStateContext) extends ClusterState {
  override def handle(context: ClusterStateContext, event: ClusterStateEvent): Unit = ???
}



