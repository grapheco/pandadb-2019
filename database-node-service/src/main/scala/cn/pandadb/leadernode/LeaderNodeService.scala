package cn.pandadb.leadernode

import org.neo4j.graphdb.{Label, Node, Relationship}


// do cluster data update
trait LeaderNodeService {
  def createNode(labels: Iterable[String] = null, properties: Map[String, Any] = null): Node
//  def updateNode(id: Long, labels: Iterable[String] = null, properties: Map[String, Any] = null): Node
}


class LeaderNodeServiceImpl() extends LeaderNodeService {

  // leader node services
  override def createNode(labels: Iterable[String], properties: Map[String, Any]): Node = {
    // begin cluster transaction
    val nodeId = getNextNodeId()
    // distribute create node to all nodes
    sendCreateNodeCommondToAllNodes(nodeId, labels, properties)
    // close cluster transaction
  }

  private def getNextNodeId(): Long = {
    1
  }

  private def sendCreateNodeCommondToAllNodes(id: Long, labels: Iterable[String], properties: Map[String, Any]): Node = {
    // get all data nodes address/port
    // send command to all data nodes
    null
  }
}
