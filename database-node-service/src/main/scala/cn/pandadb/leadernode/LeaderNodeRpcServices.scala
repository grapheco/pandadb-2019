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

case class LeaderDeleteNodeRelationship(startNodeId: Long, endNodeId: Long,
                                        relationshipName: String, direction: Direction.Value)


//zk
case class GetZkDataNodes()

//DB files
case class GetLeaderDbFileNames()

// blob
case class LeaderCreateBlobEntry(length: Long, mimeType: MimeType)

case class LeaderSaveBlob(length: Long, mimeType: MimeType)


// tx
case class LeaderBeginTransaction()
case class LeaderCommitTransaction(txId: Long)
case class LeaderCloseTransaction(txId: Long)
// node in tx
case class LeaderCreateNodeInTx(txId: Long, labels: Array[String], properties: Map[String, Any])
case class LeaderAddNodeLabelInTx(txId: Long, id: Long, label: String)
case class LeaderGetNodeByIdInTx(txId: Long, id: Long)
case class LeaderGetNodesByPropertyInTx(txId: Long, label: String, propertiesMap: Map[String, Object])
case class LeaderGetNodesByLabelInTx(txId: Long, label: String)
case class LeaderSetNodePropertyInTx(txId: Long, id: Long, propertiesMap: Map[String, Any])
case class LeaderRemoveNodeLabelInTx(txId: Long, id: Long, toDeleteLabel: String)
case class LeaderDeleteNodeInTx(txId: Long, id: Long)
case class LeaderRemoveNodePropertyInTx(txId: Long, id: Long, property: String)
//relationship in tx
case class LeaderCreateNodeRelationshipInTx(txId: Long, id1: Long, id2: Long, relationship: String, direction: Direction.Value)
case class LeaderGetNodeRelationshipsInTx(txId: Long, id: Long)
case class LeaderGetRelationshipByRelationIdInTx(txId: Long, id: Long)
case class LeaderSetRelationshipPropertyInTx(txId: Long, id: Long, propertyMap: Map[String, AnyRef])
case class LeaderDeleteRelationshipPropertiesInTx(txId: Long, id: Long, propertyArray: Array[String])
case class LeaderDeleteNodeRelationshipInTx(txId: Long, startNodeId: Long, endNodeId: Long,
                                        relationshipName: String, direction: Direction.Value)