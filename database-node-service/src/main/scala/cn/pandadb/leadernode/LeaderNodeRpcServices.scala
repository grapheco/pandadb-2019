package cn.pandadb.leadernode

import org.neo4j.graphdb.Direction

case class LeaderSayHello(msg: String)

case class LeaderRunCypher(cypher: String)

// node
case class LeaderCreateNode(labels: Array[String], properties: Map[String, Any])

case class LeaderAddNodeLabel(id: Long, label: String)

case class LeaderGetNodeById(id: Long)

case class LeaderGetNodesByProperty(label: String, propertiesMap: Map[String, Object])

case class LeaderGetNodesByLabel(label: String)

case class LeaderUpdateNodeProperty(id: Long, propertiesMap: Map[String, Any])

case class LeaderUpdateNodeLabel(id: Long, toDeleteLabel: String, newLabel: String)

case class LeaderDeleteNode(id: Long)

case class LeaderRemoveProperty(id: Long, property: String)

//// relationship
case class LeaderCreateNodeRelationship(id1: Long, id2: Long, relationship: String, direction: Direction)

case class LeaderGetNodeRelationships(id: Long)

case class LeaderGetRelationshipByRelationId(id: Long)

case class LeaderUpdateRelationshipProperty(id: Long, propertyMap: Map[String, AnyRef])

case class LeaderDeleteRelationshipProperties(id: Long, propertyArray: Array[String])

case class LeaderDeleteNodeRelationship(id: Long, relationship: String, direction: Direction)

case class LeaderGetAllDBNodes(chunkSize: Int)

case class LeaderGetAllDBRelationships(chunkSize: Int)

//zk
case class GetZkDataNodes()
