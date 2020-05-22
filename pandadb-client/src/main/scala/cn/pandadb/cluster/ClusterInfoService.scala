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
  val freshNodesPath = zkTools.buildFullZKPath("/freshNodes")
  val curator = zkTools.getCurator()
  var leaderLatch: LeaderLatch = new LeaderLatch(curator, leaderNodesPath)

  val rnd = new Random()

  def getLeaderNode(): HostAndPort = {
    try {
      HostAndPort.fromString(leaderLatch.getLeader.getId)
    } catch {
      case ex: NoNodeException => null
      case ex: Exception => throw ex
    }
  }

  def getDataNodes(): List[HostAndPort] = {
    val dataNodes = zkTools.getZKNodeChildren(dataNodesPath)
    dataNodes.map(name => {
      HostAndPort.fromString(zkTools.getZKNodeData(dataNodesPath + "/" + name))
    })
  }

  def randomGetDataNode(): HostAndPort = {
    val dataNodes = getDataNodes()
    dataNodes(rnd.nextInt(dataNodes.size))
  }
}
