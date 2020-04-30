package cn.pandadb.cluster

object ClusterInfoAPI {
  var isLeader: Boolean = true

  def getLeaderNode(): String = {"127.0.0.1"}

  def getAllDataNodes(): Iterable[String] = { Array("127.0.0.1")}
}
