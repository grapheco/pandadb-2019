package cn.pandadb.neo4j.rpc

import cn.pandadb.neo4j.driver.values.Node
import org.neo4j.graphdb.Direction

// node
case class AddNode(labels: Array[String], properties: Map[String, Any])

case class AddLabel(node: Node, label: String)

case class GetNodeById(id: Long)

case class GetNodesByProperty(label: String, propertiesMap: Map[String, Object])

case class GetNodesByLabel(label: String)

case class UpdateNodeProperty(node: Node, propertiesMap: Map[String, Any])

case class UpdateNodeLabel(node: Node, toDeleteLabel: String, newLabel: String)

case class DeleteNode(node: Node)

case class RemoveProperty(node: Node, property: String)

// relationship
case class CreateRelationshipTo(node1: Node, node2: Node, relationship: String)

case class GetNodeRelationships(node: Node)

case class DeleteRelationship(node: Node, relationship: String, direction: Direction)

// chunk stream
case class GetAllDBNodes()

case class GetAllDBRelationships()
