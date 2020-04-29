package cn.pandadb.datanode

import org.neo4j.graphdb.{GraphDatabaseService}
import org.neo4j.graphdb.{Label, Node}

// do local data update
trait DataNodeService {
  def createNodeWithId(id: Long, labels: Iterable[String] = null, properties: Map[String, Any] = null): Node
//  def updateNode(id: Long, labels: Iterable[String] = null, properties: Map[String, Any] = null): Node
  def getNode(id: Long): Node
}


class DataNodeServiceImpl(localDatabase: GraphDatabaseService) extends DataNodeService {
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
}
