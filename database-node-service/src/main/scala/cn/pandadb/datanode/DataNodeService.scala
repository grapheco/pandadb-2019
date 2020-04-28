package cn.pandadb.datanode

import org.neo4j.graphdb.{GraphDatabaseService, Result}
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.graphdb.{Label, Node, Relationship}

trait DataNodeService {
  def createNode(labels: Iterable[String] = null, properties: Map[String, Any] = null): Node
  def createNodeWithId(id: Long, labels: Iterable[String] = null, properties: Map[String, Any] = null): Node
  def getNode(id: Long): Node
}


class DataNodeServiceImpl(localDatabase: GraphDatabaseService) extends DataNodeService {
  @volatile private var isLeader = false

  def setLeader(): Unit = synchronized({
    this.isLeader = true
  })

  def cancelLeader(): Unit = synchronized({
    this.isLeader = false
  })

  override def createNodeWithId(id: Long, labels: Iterable[String], properties: Map[String, Any]): Node = {
    val tx = localDatabase.beginTx()
    val node = localDatabase.createNode(id)
    if (labels != null) {
      labels.foreach(labelName => {node.addLabel(Label.label(labelName))})
    }
    if (properties != null) {
      properties.map((x) => node.setProperty(x._1, x._2))
    }
    tx.success()
    tx.close()
    node
  }

  override def getNode(id: Long): Node = {
    localDatabase.getNodeById(id)
  }

  // leader node services
  override def createNode(labels: Iterable[String], properties: Map[String, Any]): Node = {
    if (!isLeader) {
      throw new Exception("slave data node do not support this function")
    }
    val nodeId = getNextNodeId()
    // distribute create node to all nodes
    sendCreateNodeCommondToAllNodes(nodeId, labels, properties)
  }

  private def getNextNodeId(): Long = {
    1
  }

  private def sendCreateNodeCommondToAllNodes(id: Long, labels: Iterable[String], properties: Map[String, Any]): Node = {
    // get all data nodes address/port
    // send command to all data nodes
    createNodeWithId(id, labels, properties)
  }
}
