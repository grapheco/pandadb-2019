package cn.pandadb.datanode

import cn.pandadb.driver.values.{Direction => PandaDirection}
import scala.collection.mutable.ArrayBuffer

case class SayHello(msg: String)

case class RunCypher(cypher: String)

// node
case class CreateNode(id: Long, labels: Array[String], properties: Map[String, Any])

case class AddNodeLabel(id: Long, label: String)

case class GetNodeById(id: Long)

case class GetNodesByProperty(label: String, propertiesMap: Map[String, Object])

case class GetNodesByLabel(label: String)

case class SetNodeProperty(id: Long, propertiesMap: Map[String, Any])

case class RemoveNodeLabel(id: Long, toDeleteLabel: String)

case class DeleteNode(id: Long)

case class RemoveNodeProperty(id: Long, property: String)

// relationship
case class CreateNodeRelationship(rId: ArrayBuffer[Long], id1: Long, id2: Long,
                                  relationship: String, direction: PandaDirection.Value)

case class GetNodeRelationships(id: Long)

case class DeleteNodeRelationship(startNodeId: Long, endNodeId: Long,
                                  relationshipName: String, direction: PandaDirection.Value)

case class GetRelationshipByRelationId(id: Long)

case class SetRelationshipProperty(id: Long, propertyMap: Map[String, AnyRef])

case class DeleteRelationshipProperties(id: Long, propertyArray: Array[String])

case class GetAllDBNodes(chunkSize: Int)

case class GetAllDBRelationships(chunkSize: Int)

case class GetAllDBLabels(chunkSize: Int)

case class ReadDbFileRequest(name: String)

case class ReadCompressedDbFileRequest(zipFileName: String)


// tx
case class BeginTransaction(txId: Long)
case class CommitTransaction(txId: Long)
case class CloseTransaction(txId: Long)

// node in tx
case class CreateNodeInTx(txId: Long, id: Long, labels: Array[String], properties: Map[String, Any])
case class AddNodeLabelInTx(txId: Long, id: Long, label: String)
case class GetNodeByIdInTx(txId: Long, id: Long)
case class GetNodesByPropertyInTx(txId: Long, label: String, propertiesMap: Map[String, Object])
case class GetNodesByLabelInTx(txId: Long, label: String)
case class SetNodePropertyInTx(txId: Long, id: Long, propertiesMap: Map[String, Any])
case class RemoveNodeLabelInTx(txId: Long, id: Long, toDeleteLabel: String)
case class DeleteNodeInTx(txId: Long, id: Long)
case class RemoveNodePropertyInTx(txId: Long, id: Long, property: String)
// relationship in tx
case class CreateNodeRelationshipInTx(txId: Long, rId: ArrayBuffer[Long], id1: Long, id2: Long,
                                  relationship: String, direction: PandaDirection.Value)
case class GetNodeRelationshipsInTx(txId: Long, id: Long)
case class DeleteNodeRelationshipInTx(txId: Long, startNodeId: Long, endNodeId: Long,
                                  relationshipName: String, direction: PandaDirection.Value)
case class GetRelationshipByRelationIdInTx(txId: Long, id: Long)
case class SetRelationshipPropertyInTx(txId: Long, id: Long, propertyMap: Map[String, AnyRef])
case class DeleteRelationshipPropertiesInTx(txId: Long, id: Long, propertyArray: Array[String])
case class GetAllDBNodesInTx(txId: Long, chunkSize: Int)
case class GetAllDBRelationshipsInTx(txId: Long, chunkSize: Int)
case class GetAllDBLabelsInTx(txId: Long, chunkSize: Int)