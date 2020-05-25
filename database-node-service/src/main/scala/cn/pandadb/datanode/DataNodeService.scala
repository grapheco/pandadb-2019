package cn.pandadb.datanode

import java.io.{File, FileInputStream}

import cn.pandadb.driver.result.InternalRecords
import cn.pandadb.driver.values.{Node, Relationship}
import cn.pandadb.util.{PandaReplyMessage, ValueConverter}
import io.netty.buffer.ByteBuf
import org.grapheco.hippo.ChunkedStream
import org.neo4j.graphdb.{Direction, GraphDatabaseService, Label, RelationshipType, Relationship => DbRelationship}

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

  def updateNodeProperty(id: Long, propertiesMap: Map[String, Any]): PandaReplyMessage.Value

  def updateNodeLabel(id: Long, toDeleteLabel: String, newLabel: String): PandaReplyMessage.Value

  def deleteNode(id: Long): PandaReplyMessage.Value

  def removeProperty(id: Long, property: String): PandaReplyMessage.Value

  def createNodeRelationshipLeader(id1: Long, id2: Long, relationship: String, direction: Direction): ArrayBuffer[Relationship]

  def createNodeRelationshipFollower(relationId: ArrayBuffer[Long], id1: Long, id2: Long, relationship: String, direction: Direction): PandaReplyMessage.Value

  def getNodeRelationships(id: Long): ArrayBuffer[Relationship]

  def deleteNodeRelationship(id: Long, relationship: String, direction: Direction): PandaReplyMessage.Value

  def getRelationshipByRelationId(id: Long): Relationship

  def updateRelationshipProperty(id: Long, propertyMap: Map[String, AnyRef]): PandaReplyMessage.Value

  def deleteRelationshipProperties(id: Long, propertyArray: Array[String]): PandaReplyMessage.Value


  def getAllDBNodes(chunkSize: Int): ChunkedStream

  def getAllDBRelationships(chunkSize: Int): ChunkedStream

  def getDbFile(path: String, name: String): ChunkedStream
}


class DataNodeServiceImpl(localDatabase: GraphDatabaseService) extends DataNodeService {

  override def getRelationshipByRelationId(id: Long): Relationship = {
    val tx = localDatabase.beginTx()
    val relation = localDatabase.getRelationshipById(id)
    val driverRelation = ValueConverter.toDriverRelationship(relation)
    tx.success()
    tx.close()
    driverRelation
  }

  override def updateRelationshipProperty(id: Long, propertyMap: Map[String, AnyRef]): PandaReplyMessage.Value = {
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

  override def updateNodeProperty(id: Long, propertiesMap: Map[String, Any]): PandaReplyMessage.Value = {
    val tx = localDatabase.beginTx()
    val db_node = localDatabase.getNodeById(id)
    for (map <- propertiesMap) {
      db_node.setProperty(map._1, map._2)
    }
    tx.success()
    tx.close()
    PandaReplyMessage.SUCCESS
  }

  override def updateNodeLabel(id: Long, toDeleteLabel: String, newLabel: String): PandaReplyMessage.Value = {
    val tx = localDatabase.beginTx()
    val db_node = localDatabase.getNodeById(id)
    db_node.removeLabel(Label.label(toDeleteLabel))
    db_node.addLabel(Label.label(newLabel))
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

  override def removeProperty(id: Long, property: String): PandaReplyMessage.Value = {
    val tx = localDatabase.beginTx()
    val db_node = localDatabase.getNodeById(id)
    db_node.removeProperty(property)
    tx.success()
    tx.close()
    PandaReplyMessage.SUCCESS
  }

  override def createNodeRelationshipLeader(id1: Long, id2: Long, relationship: String, direction: Direction): ArrayBuffer[Relationship] = {
    val tx = localDatabase.beginTx()
    val dbNode1 = localDatabase.getNodeById(id1)
    val dbNode2 = localDatabase.getNodeById(id2)
    var relation1: DbRelationship = null
    var relation2: DbRelationship = null
    val relationList = ArrayBuffer[Relationship]()
    direction match {
      case Direction.BOTH => {
        relation1 = dbNode1.createRelationshipTo(dbNode2, RelationshipType.withName(relationship))
        relation2 = dbNode2.createRelationshipTo(dbNode1, RelationshipType.withName(relationship))
      }
      case Direction.INCOMING => {
        relation1 = dbNode2.createRelationshipTo(dbNode1, RelationshipType.withName(relationship))
      }
      case Direction.OUTGOING => {
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

  override def createNodeRelationshipFollower(relationId: ArrayBuffer[Long], id1: Long, id2: Long, relationship: String, direction: Direction): PandaReplyMessage.Value = {
    val tx = localDatabase.beginTx()
    val dbNode1 = localDatabase.getNodeById(id1)
    val dbNode2 = localDatabase.getNodeById(id2)
    direction match {
      case Direction.BOTH => {
        dbNode1.createRelationshipTo(dbNode2, RelationshipType.withName(relationship), relationId(0))
        dbNode2.createRelationshipTo(dbNode1, RelationshipType.withName(relationship), relationId(1))
      }
      case Direction.INCOMING => {
        dbNode2.createRelationshipTo(dbNode1, RelationshipType.withName(relationship), relationId(0))
      }
      case Direction.OUTGOING => {
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

  override def deleteNodeRelationship(id: Long, relationship: String, direction: Direction): PandaReplyMessage.Value = {
    val tx = localDatabase.beginTx()
    val db_node = localDatabase.getNodeById(id)
    val relation = db_node.getSingleRelationship(RelationshipType.withName(relationship), direction)
    relation.delete()
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
