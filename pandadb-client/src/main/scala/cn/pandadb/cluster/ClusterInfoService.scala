package cn.pandadb.cluster

import org.apache.curator.framework.recipes.leader.LeaderLatch
import org.apache.curator.shaded.com.google.common.net.HostAndPort
import org.apache.zookeeper.KeeperException.NoNodeException
import org.slf4j.Logger

import scala.util.Random

class ClusterInfoService(zkTools: ZKTools) {

  val leaderNodesPath = zkTools.buildFullZKPath("/leaderNodes")
  val dataNodesPath = zkTools.buildFullZKPath("/dataNodes")
  val dataVersionPath = zkTools.buildFullZKPath("/dataVersion")

  val curator = zkTools.getCurator()
  var leaderLatch: LeaderLatch = new LeaderLatch(curator, leaderNodesPath)

  val rnd = new Random()

  private def _getLeaderNode(): String = {
    val leaderNode = zkTools.getZKNodeChildren(leaderNodesPath)
    if (leaderNode isEmpty) null else leaderNode.head
  }
  def getLeaderNodeAddress(): String = {
    _getLeaderNode()
  }

  def getLeaderNode(): HostAndPort = {
    val leaderNode = _getLeaderNode()
    if (leaderNode == null) throw new Exception("Cluster has not leader.")
    try {
      HostAndPort.fromString(leaderNode)
    } catch {
      case ex: NoNodeException => null
      case ex: Exception => throw ex
    }
  }

  def getDataNodes(): List[HostAndPort] = {
    val dataNodes = zkTools.getZKNodeChildren(dataNodesPath)
    dataNodes.map(name => {
      //      HostAndPort.fromString(zkTools.getZKNodeData(dataNodesPath + "/" + name))
      HostAndPort.fromString(name)
    })
  }

  def randomGetReadNode(): HostAndPort = {
    val dataNodes = getDataNodes()
    if (dataNodes.size == 0) getLeaderNode()
    else dataNodes(rnd.nextInt(dataNodes.size))
  }
}
