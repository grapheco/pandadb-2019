package cn.pandadb.datanode

import java.io.{File, FileInputStream}

import cn.pandadb.driver.result.InternalRecords
import cn.pandadb.driver.values.{Node, Relationship}
import cn.pandadb.util.{PandaReplyMessage, ValueConverter}
import io.netty.buffer.ByteBuf
import org.grapheco.hippo.ChunkedStream
import org.neo4j.graphdb.{Node => DbNode, Direction, GraphDatabaseService, Label, RelationshipType, Relationship => DbRelationship}
import cn.pandadb.driver.values.{Direction => PandaDirection}
import scala.collection.JavaConversions
import scala.collection.mutable.ArrayBuffer

// do local data update
trait DataNodeService {
  def sayHello(msg: String): PandaReplyMessage.Value

  def runCypher(cypher: String): InternalRecords

  def createNodeLeader(labels: Array[String], properties: Map[String, Any]): Node

  def createNodeFollower(id: Long, labels: Array[String], properties: Map[String, Any]): PandaReplyMessage.Value

  def addNodeLabel(id: Long, label: String): PandaReplyMessage.Value

  def getNodeById(id: Long): Node

  def getNodesByProperty(label: String, propertiesMap: Map[String, Object]): ArrayBuffer[Node]

  def getNodesByLabel(label: String): ArrayBuffer[Node]

  def setNodeProperty(id: Long, propertiesMap: Map[String, Any]): PandaReplyMessage.Value

  def removeNodeLabel(id: Long, toDeleteLabel: String): PandaReplyMessage.Value

  def deleteNode(id: Long): PandaReplyMessage.Value

  def removeNodeProperty(id: Long, property: String): PandaReplyMessage.Value

  def createNodeRelationshipLeader(id1: Long, id2: Long, relationship: String, direction: PandaDirection.Value): ArrayBuffer[Relationship]

  def createNodeRelationshipFollower(relationId: ArrayBuffer[Long], id1: Long, id2: Long, relationship: String, direction: PandaDirection.Value): PandaReplyMessage.Value

  def getNodeRelationships(id: Long): ArrayBuffer[Relationship]

  def deleteNodeRelationship(startNodeId: Long, endNodeId: Long, relationshipName: String, direction: PandaDirection.Value): PandaReplyMessage.Value

  def getRelationshipById(id: Long): Relationship

  def setRelationshipProperty(id: Long, propertyMap: Map[String, AnyRef]): PandaReplyMessage.Value

  def deleteRelationshipProperties(id: Long, propertyArray: Array[String]): PandaReplyMessage.Value


  def getAllDBNodes(chunkSize: Int): ChunkedStream

  def getAllDBRelationships(chunkSize: Int): ChunkedStream

  def getAllDBLabels(chunkSize: Int): ChunkedStream

  def getDbFile(path: String, name: String): ChunkedStream
}


class DataNodeServiceImpl(localDatabase: GraphDatabaseService) extends DataNodeService {

  override def getRelationshipById(id: Long): Relationship = {
    val tx = localDatabase.beginTx()
    val relation = localDatabase.getRelationshipById(id)
    val driverRelation = ValueConverter.toDriverRelationship(relation)
    tx.success()
    tx.close()
    driverRelation
  }

  override def setRelationshipProperty(id: Long, propertyMap: Map[String, AnyRef]): PandaReplyMessage.Value = {
    val tx = localDatabase.beginTx()
    val relation = localDatabase.getRelationshipById(id)
    propertyMap.foreach(m => relation.setProperty(m._1, m._2))
    tx.success()
    tx.close()
    PandaReplyMessage.SUCCESS
  }

  override def deleteRelationshipProperties(id: Long, propertyArray: Array[String]): PandaReplyMessage.Value = {
    val tx = localDatabase.beginTx()
    val relation = localDatabase.getRelationshipById(id)
    propertyArray.foreach(p => relation.removeProperty(p))
    tx.success()
    tx.close()
    PandaReplyMessage.SUCCESS
  }

  override def sayHello(msg: String): PandaReplyMessage.Value = {
    PandaReplyMessage.SUCCESS
  }

  override def runCypher(cypher: String): InternalRecords = {
    val tx = localDatabase.beginTx()
    val result = localDatabase.execute(cypher)
    val internalRecords = ValueConverter.neo4jResultToDriverRecords(result)
    tx.success()
    tx.close()
    internalRecords
  }

  override def createNodeLeader(labels: Array[String], properties: Map[String, Any]): Node = {
    val tx = localDatabase.beginTx()
    val node = localDatabase.createNode()
    for (labelName <- labels) {
      val label = Label.label(labelName)
      node.addLabel(label)
    }
    properties.foreach(x => {
      node.setProperty(x._1, x._2)
    })
    val driverNode = ValueConverter.toDriverNode(node)
    tx.success()
    tx.close()
    driverNode
  }

  override def createNodeFollower(id: Long, labels: Array[String], properties: Map[String, Any]): PandaReplyMessage.Value = {
    val tx = localDatabase.beginTx()
    val node = localDatabase.createNode(id)
    for (labelName <- labels) {
      val label = Label.label(labelName)
      node.addLabel(label)
    }
    properties.foreach(x => {
      node.setProperty(x._1, x._2)
    })
    tx.success()
    tx.close()
    PandaReplyMessage.SUCCESS
  }

  override def addNodeLabel(id: Long, label: String): PandaReplyMessage.Value = {
    val tx = localDatabase.beginTx()
    val node = localDatabase.getNodeById(id)
    node.addLabel(Label.label(label))
    //    val driverNode = ValueConverter.toDriverNode(node)
    tx.success()
    tx.close()
    PandaReplyMessage.SUCCESS
  }

  override def getNodeById(id: Long): Node = {
    val tx = localDatabase.beginTx()
    val node = localDatabase.getNodeById(id)
    val driverNode = ValueConverter.toDriverNode(node)
    tx.success()
    tx.close()
    driverNode
  }

  override def getNodesByProperty(label: String, propertiesMap: Map[String, Object]): ArrayBuffer[Node] = {
    val lst = ArrayBuffer[Node]()
    val tx = localDatabase.beginTx()
    val _label = Label.label(label)
    val propertyMaps = JavaConversions.mapAsJavaMap(propertiesMap)
    val res = localDatabase.findNodes(_label, propertyMaps)
    while (res.hasNext) {
      val node = res.next()
      val driverNode = ValueConverter.toDriverNode(node)
      lst += driverNode
    }
    tx.success()
    tx.close()
    lst
  }

  override def getNodesByLabel(label: String): ArrayBuffer[Node] = {
    val lst = ArrayBuffer[Node]()
    val tx = localDatabase.beginTx()
    val _label = Label.label(label)
    val res = localDatabase.findNodes(_label)
    while (res.hasNext) {
      val node = res.next()
      val driverNode = ValueConverter.toDriverNode(node)
      lst += driverNode
    }
    tx.success()
    tx.close()
    lst
  }

  override def setNodeProperty(id: Long, propertiesMap: Map[String, Any]): PandaReplyMessage.Value = {
    val tx = localDatabase.beginTx()
    val db_node = localDatabase.getNodeById(id)
    for (map <- propertiesMap) {
      db_node.setProperty(map._1, map._2)
    }
    tx.success()
    tx.close()
    PandaReplyMessage.SUCCESS
  }

  override def removeNodeLabel(id: Long, toDeleteLabel: String): PandaReplyMessage.Value = {
    val tx = localDatabase.beginTx()
    val db_node = localDatabase.getNodeById(id)
    db_node.removeLabel(Label.label(toDeleteLabel))
    tx.success()
    tx.close()
    PandaReplyMessage.SUCCESS
  }

  override def deleteNode(id: Long): PandaReplyMessage.Value = {
    val tx = localDatabase.beginTx()
    val db_node = localDatabase.getNodeById(id)
    if (db_node.hasRelationship) {
      val relationships = db_node.getRelationships().iterator()
      while (relationships.hasNext) {
        relationships.next().delete()
      }
    }
    db_node.delete()
    tx.success()
    tx.close()
    PandaReplyMessage.SUCCESS
  }

  override def removeNodeProperty(id: Long, property: String): PandaReplyMessage.Value = {
    val tx = localDatabase.beginTx()
    val db_node = localDatabase.getNodeById(id)
    db_node.removeProperty(property)
    tx.success()
    tx.close()
    PandaReplyMessage.SUCCESS
  }

  override def createNodeRelationshipLeader(id1: Long, id2: Long, relationship: String, direction: PandaDirection.Value): ArrayBuffer[Relationship] = {
    val tx = localDatabase.beginTx()
    val dbNode1 = localDatabase.getNodeById(id1)
    val dbNode2 = localDatabase.getNodeById(id2)
    var relation1: DbRelationship = null
    var relation2: DbRelationship = null
    val relationList = ArrayBuffer[Relationship]()
    direction match {
      case PandaDirection.BOTH => {
        relation1 = dbNode1.createRelationshipTo(dbNode2, RelationshipType.withName(relationship))
        relation2 = dbNode2.createRelationshipTo(dbNode1, RelationshipType.withName(relationship))
      }
      case PandaDirection.INCOMING => {
        relation1 = dbNode2.createRelationshipTo(dbNode1, RelationshipType.withName(relationship))
      }
      case PandaDirection.OUTGOING => {
        relation1 = dbNode1.createRelationshipTo(dbNode2, RelationshipType.withName(relationship))
      }
    }
    if (relation2 != null) {
      val driverRelation1 = ValueConverter.toDriverRelationship(relation1)
      val driverRelation2 = ValueConverter.toDriverRelationship(relation2)
      relationList += driverRelation1
      relationList += driverRelation2
    } else {
      val driverRelation1 = ValueConverter.toDriverRelationship(relation1)
      relationList += driverRelation1
    }
    tx.success()
    tx.close()
    relationList
  }

  override def createNodeRelationshipFollower(relationId: ArrayBuffer[Long], id1: Long, id2: Long, relationship: String, direction: PandaDirection.Value): PandaReplyMessage.Value = {
    val tx = localDatabase.beginTx()
    val dbNode1 = localDatabase.getNodeById(id1)
    val dbNode2 = localDatabase.getNodeById(id2)
    direction match {
      case PandaDirection.BOTH => {
        dbNode1.createRelationshipTo(dbNode2, RelationshipType.withName(relationship), relationId(0))
        dbNode2.createRelationshipTo(dbNode1, RelationshipType.withName(relationship), relationId(1))
      }
      case PandaDirection.INCOMING => {
        dbNode2.createRelationshipTo(dbNode1, RelationshipType.withName(relationship), relationId(0))
      }
      case PandaDirection.OUTGOING => {
        dbNode1.createRelationshipTo(dbNode2, RelationshipType.withName(relationship), relationId(0))
      }
    }
    tx.success()
    tx.close()
    PandaReplyMessage.SUCCESS
  }

  override def getNodeRelationships(id: Long): ArrayBuffer[Relationship] = {
    val lst = ArrayBuffer[Relationship]()
    val tx = localDatabase.beginTx()
    val db_node = localDatabase.getNodeById(id)
    val relationships = db_node.getRelationships()
    val relationIter = relationships.iterator()
    while (relationIter.hasNext) {
      val relation = relationIter.next()
      val driverRelation = ValueConverter.toDriverRelationship(relation)
      lst += driverRelation
    }
    tx.success()
    tx.close()
    lst
  }

  override def deleteNodeRelationship(startNodeId: Long, endNodeId: Long, relationshipName: String, direction: PandaDirection.Value): PandaReplyMessage.Value = {
    val tx = localDatabase.beginTx()
    val db_node: DbNode = localDatabase.getNodeById(startNodeId)
    var resIter: java.util.Iterator[DbRelationship] = null
    var resIter2: java.util.Iterator[DbRelationship] = null

    direction match {
      case PandaDirection.BOTH => {
        resIter = db_node.getRelationships(RelationshipType.withName(relationshipName), Direction.BOTH).iterator()
        resIter2 = localDatabase.getNodeById(endNodeId).getRelationships(RelationshipType.withName(relationshipName), Direction.BOTH).iterator()
      }
      case PandaDirection.INCOMING => {
        resIter = db_node.getRelationships(RelationshipType.withName(relationshipName), Direction.INCOMING).iterator()
      }
      case PandaDirection.OUTGOING => {
        resIter = db_node.getRelationships(RelationshipType.withName(relationshipName), Direction.OUTGOING).iterator()
      }
    }
    // This code should be optimized
    while (resIter.hasNext) {
      val relation = resIter.next()
      if (relation.getEndNodeId == endNodeId) {
        relation.delete()
      }
    }
    if (direction == PandaDirection.BOTH) {
      while (resIter2.hasNext) {
        val relation = resIter2.next()
        if (relation.getEndNodeId == startNodeId) {
          relation.delete()
        }
      }
    }
    tx.success()
    tx.close()
    PandaReplyMessage.SUCCESS
  }

  override def getAllDBNodes(chunkSize: Int): ChunkedStream = {
    val tx = localDatabase.beginTx()
    val nodesIter = localDatabase.getAllNodes.stream().iterator()
    val iterable = JavaConversions.asScalaIterator(nodesIter).toIterable
    ChunkedStream.grouped(chunkSize, iterable.map(x => ValueConverter.toDriverNode(x)), {
      tx.success()
      tx.close()
    })
  }

  override def getAllDBRelationships(chunkSize: Int): ChunkedStream = {
    val tx = localDatabase.beginTx()
    val relationIter = localDatabase.getAllRelationships.stream().iterator()
    val iterable = JavaConversions.asScalaIterator(relationIter).toIterable
    ChunkedStream.grouped(chunkSize, iterable.map(x => ValueConverter.toDriverRelationship(x)), {
      tx.success()
      tx.close()
    })
  }

  override def getAllDBLabels(chunkSize: Int): ChunkedStream = {
    val tx = localDatabase.beginTx()
    val labelIter = localDatabase.getAllLabels.stream().iterator()
    val iterable = JavaConversions.asScalaIterator(labelIter).toIterable
    ChunkedStream.grouped(chunkSize, iterable.map(x => ValueConverter.convertLabel(x)), {
      tx.success()
      tx.close()
    })
  }

  override def getDbFile(path: String, name: String): ChunkedStream = {
    new ChunkedStream {
      val filePath = path + "/" + name
      val fis = new FileInputStream(new File(filePath))
      val length = new File(path).length()
      var count = 0

      override def hasNext(): Boolean = {
        count < length
      }

      override def nextChunk(buf: ByteBuf): Unit = {
        val written = buf.writeBytes(fis, 1024 * 1024 * 10)
        count += written
      }

      override def close(): Unit = {
        fis.close()
      }
    }
  }
}
