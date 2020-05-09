package cn.pandadb.cluster

trait NodeRoleChangedEvent {}

case class LeaderNodeChangedEvent(isLeader: Boolean, leaderNode: String) extends NodeRoleChangedEvent {}

trait NodeRoleChangedEventListener {
  def notifyRoleChanged(event: NodeRoleChangedEvent): Unit
}
