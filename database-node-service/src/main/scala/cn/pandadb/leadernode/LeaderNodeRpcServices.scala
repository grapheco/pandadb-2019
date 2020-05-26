package cn.pandadb.leadernode

import cn.pandadb.blob.MimeType
import cn.pandadb.driver.values.Direction

case class LeaderSayHello(msg: String)

case class LeaderRunCypher(cypher: String)

// node
case class LeaderCreateNode(labels: Array[String], properties: Map[String, Any])

case class LeaderAddNodeLabel(id: Long, label: String)

case class LeaderGetNodeById(id: Long)

case class LeaderGetNodesByProperty(label: String, propertiesMap: Map[String, Object])

case class LeaderGetNodesByLabel(label: String)

case class LeaderSetNodeProperty(id: Long, propertiesMap: Map[String, Any])

case class LeaderRemoveNodeLabel(id: Long, toDeleteLabel: String)

case class LeaderDeleteNode(id: Long)

case class LeaderRemoveNodeProperty(id: Long, property: String)

//// relationship
case class LeaderCreateNodeRelationship(id1: Long, id2: Long, relationship: String, direction: Direction.Value)

case class LeaderGetNodeRelationships(id: Long)

case class LeaderGetRelationshipByRelationId(id: Long)

case class LeaderSetRelationshipProperty(id: Long, propertyMap: Map[String, AnyRef])

case class LeaderDeleteRelationshipProperties(id: Long, propertyArray: Array[String])

case class LeaderDeleteNodeRelationship(startNodeId: Long, endNodeId: Long, relationshipName: String, direction: Direction.Value)

//case class LeaderGetAllDBNodes(chunkSize: Int)
//case class LeaderGetAllDBRelationships(chunkSize: Int)
//case class LeaderGetAllDBLabels(chunkSize:Int)

//zk
case class GetZkDataNodes()

//DB files
case class GetLeaderDbFileNames()

// blob

case class LeaderCreateBlobEntry(length: Long, mimeType: MimeType)
