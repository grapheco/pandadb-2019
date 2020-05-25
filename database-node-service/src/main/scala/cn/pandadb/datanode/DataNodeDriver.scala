package cn.pandadb.datanode

import java.io.{File, FileOutputStream, InputStream}

import cn.pandadb.configuration.Config
import cn.pandadb.driver.result.InternalRecords
import cn.pandadb.driver.values.{Node, Relationship}
import cn.pandadb.util.PandaReplyMessage
import net.neoremind.kraps.rpc.netty.HippoEndpointRef
import org.neo4j.graphdb.Direction

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

class DataNodeDriver {
  val config = new Config
  val logger = config.getLogger(this.getClass)

  def checkFileIsExist(path: String): Unit = {
    val dbFile = new File(path)
    if (!dbFile.exists()) {
      dbFile.mkdirs
    }
  }

  def pullFile(dbPath: String, fileNames: ArrayBuffer[String], endpointRef: HippoEndpointRef, duration: Duration): PandaReplyMessage.Value = {
    checkFileIsExist(dbPath)
    fileNames.foreach(name => {
      val filePath = dbPath + name
      val checkPath = filePath.substring(0, filePath.lastIndexOf("/"))
      checkFileIsExist(checkPath)
      val fis = endpointRef.getInputStream(ReadDbFileRequest(name), Duration.Inf)
      val fos = new FileOutputStream(filePath)
      val buffer = new Array[Byte](1024)
      var size = 0
      while (size != -1) {
        fos.write(buffer, 0, size)
        size = fis.read(buffer)
      }
      fos.close()
      fis.close()
      logger.info(s"download file: $name")
    })
    PandaReplyMessage.SUCCESS
  }

  def runCypher(cypher: String, endpointRef: HippoEndpointRef, duration: Duration): InternalRecords = {
    val res = Await.result(endpointRef.askWithBuffer[InternalRecords](RunCypher(cypher)), duration)
    res
  }

  def sayHello(msg: String, endpointRef: HippoEndpointRef, duration: Duration): PandaReplyMessage.Value = {
    val res = Await.result(endpointRef.askWithBuffer[PandaReplyMessage.Value](SayHello(msg)), duration)
    res
  }

  def createNode(id: Long, labels: Array[String], properties: Map[String, Any], endpointRef: HippoEndpointRef, duration: Duration): Node = {
    val res = Await.result(endpointRef.askWithBuffer[Node](CreateNode(id, labels, properties)), duration)
    res
  }

  def addNodeLabel(id: Long, label: String, endpointRef: HippoEndpointRef, duration: Duration): PandaReplyMessage.Value = {
    val res = Await.result(endpointRef.askWithBuffer[PandaReplyMessage.Value](AddNodeLabel(id, label)), duration)
    res
  }

  def getNodeById(id: Long, endpointRef: HippoEndpointRef, duration: Duration): Node = {
    val node = Await.result(endpointRef.askWithBuffer[Node](GetNodeById(id)), duration)
    node
  }

  def getNodesByProperty(label: String, propertiesMap: Map[String, Any], endpointRef: HippoEndpointRef, duration: Duration): ArrayBuffer[Node] = {
    implicit def any2Object(x: Map[String, Any]): Map[String, Object] = x.asInstanceOf[Map[String, Object]]

    val res = Await.result(endpointRef.askWithBuffer[ArrayBuffer[Node]](GetNodesByProperty(label, propertiesMap)), duration)
    res
  }

  def getNodesByLabel(label: String, endpointRef: HippoEndpointRef, duration: Duration): ArrayBuffer[Node] = {
    val res = Await.result(endpointRef.askWithBuffer[ArrayBuffer[Node]](GetNodesByLabel(label)), duration)
    res
  }

  def updateNodeProperty(id: Long, propertiesMap: Map[String, Any], endpointRef: HippoEndpointRef, duration: Duration): PandaReplyMessage.Value = {
    val res = Await.result(endpointRef.askWithBuffer[PandaReplyMessage.Value](UpdateNodeProperty(id, propertiesMap)), duration)
    res
  }

  def updateNodeLabel(id: Long, toDeleteLabel: String, newLabel: String, endpointRef: HippoEndpointRef, duration: Duration): PandaReplyMessage.Value = {
    val res = Await.result(endpointRef.askWithBuffer[PandaReplyMessage.Value](UpdateNodeLabel(id, toDeleteLabel, newLabel)), duration)
    res
  }

  def deleteNode(id: Long, endpointRef: HippoEndpointRef, duration: Duration): PandaReplyMessage.Value = {
    val res = Await.result(endpointRef.askWithBuffer[PandaReplyMessage.Value](DeleteNode(id)), duration)
    res
  }

  def removeProperty(id: Long, property: String, endpointRef: HippoEndpointRef, duration: Duration): PandaReplyMessage.Value = {
    val res = Await.result(endpointRef.askWithBuffer[PandaReplyMessage.Value](RemoveProperty(id, property)), duration)
    res
  }

  def createNodeRelationship(id1: Long, id2: Long, relationship: String, direction: Direction, endpointRef: HippoEndpointRef, duration: Duration): PandaReplyMessage.Value = {
    val res = Await.result(endpointRef.askWithBuffer[PandaReplyMessage.Value](CreateNodeRelationship(id1, id2, relationship, direction)), duration)
    res
  }

  def getNodeRelationships(id: Long, endpointRef: HippoEndpointRef, duration: Duration): ArrayBuffer[Relationship] = {
    val res = Await.result(endpointRef.askWithBuffer[ArrayBuffer[Relationship]](GetNodeRelationships(id)), duration)
    res
  }

  def deleteNodeRelationship(id: Long, relationship: String, direction: Direction, endpointRef: HippoEndpointRef, duration: Duration): PandaReplyMessage.Value = {
    val res = Await.result(endpointRef.askWithBuffer[PandaReplyMessage.Value](DeleteNodeRelationship(id, relationship, direction)), duration)
    res
  }

  def getAllDBNodes(chunkSize: Int, endpointRef: HippoEndpointRef, duration: Duration): Stream[Node] = {
    val res = endpointRef.getChunkedStream[Node](GetAllDBNodes(chunkSize), duration)
    res
  }

  def getAllDBRelationships(chunkSize: Int, endpointRef: HippoEndpointRef, duration: Duration): Stream[Relationship] = {
    val res = endpointRef.getChunkedStream[Relationship](GetAllDBRelationships(chunkSize), duration)
    res
  }

  def getRelationshipByRelationId(relationId: Long, endpointRef: HippoEndpointRef, duration: Duration): Relationship = {
    val res = Await.result(endpointRef.askWithBuffer[Relationship](GetRelationshipByRelationId(relationId)), duration)
    res
  }

  def updateRelationshipProperty(relationId: Long, propertyMap: Map[String, AnyRef], endpointRef: HippoEndpointRef, duration: Duration): PandaReplyMessage.Value = {
    val res = Await.result(endpointRef.askWithBuffer[PandaReplyMessage.Value](UpdateRelationshipProperty(relationId, propertyMap)), duration)
    res
  }

  def deleteRelationshipProperties(relationId: Long, propertyArray: Array[String], endpointRef: HippoEndpointRef, duration: Duration): PandaReplyMessage.Value = {
    val res = Await.result(endpointRef.askWithBuffer[PandaReplyMessage.Value](DeleteRelationshipProperties(relationId, propertyArray)), duration)
    res
  }

}
