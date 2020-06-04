package cn.pandadb.client

import java.io.{File, FileInputStream}
import java.nio.ByteBuffer

import cn.pandadb.blob.{BlobEntry, MimeType}
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnvClientConfig}
import net.neoremind.kraps.rpc.netty.HippoRpcEnvFactory
import cn.pandadb.datanode.DataNodeDriver
import cn.pandadb.leadernode.LeaderNodeDriver
import cn.pandadb.cluster.{ClusterInfoService, ZKTools}
import cn.pandadb.driver.result.InternalRecords
import cn.pandadb.util.PandaReplyMessage
import cn.pandadb.driver.values.{Direction, Node, Relationship}
import org.apache.curator.shaded.com.google.common.net.HostAndPort

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import cn.pandadb.driver.values.Direction.Value
import org.neo4j.cypher.internal.compiler.v2_3.commands.RelationshipById

class PandaDBClient(zkAddress: String) extends AutoCloseable {

  private val rpcServerName = "pandadb-client"
  private val LeaderNodeEndpointName = "leader-node-endpoint"
  private val DataNodeEndpointName = "data-node-endpoint"
  private val pandaDBZkDir = "/pandadb/v0.0.0/"

  private val zkTools = new ZKTools(zkAddress, pandaDBZkDir)
  private val clusterInfoService = new ClusterInfoService(zkTools)
  private val clientConfig = RpcEnvClientConfig(new RpcConf(), rpcServerName)
  private val clientRpcEnv = HippoRpcEnvFactory.create(clientConfig)

  private val leaderNode: HostAndPort = clusterInfoService.getLeaderNode()

  private val leaderNodeEndpointRef = clientRpcEnv.setupEndpointRef(
    new RpcAddress(leaderNode.getHostText, leaderNode.getPort), LeaderNodeEndpointName)
  private val leaderNodeDriver = new LeaderNodeDriver

  private val dataNode: HostAndPort = clusterInfoService.randomGetReadNode()
  println(leaderNode, dataNode)

  private val dataNodeEndpointRef = clientRpcEnv.setupEndpointRef(
    new RpcAddress(dataNode.getHostText, dataNode.getPort), DataNodeEndpointName)

  private val dataNodeDriver = new DataNodeDriver

  /// update functions

  def createNode(labels: Array[String], properties: Map[String, Any]): Node = {
    val res = leaderNodeDriver.createNode(labels, properties, leaderNodeEndpointRef, Duration.Inf)
    res match {
      case n: Node => n
      case _ => throw new Exception("Return Type Invalid")
    }
  }

  def deleteNode(id: Long): PandaReplyMessage.Value = {
    val res = leaderNodeDriver.deleteNode(id, leaderNodeEndpointRef, Duration.Inf)
    res match {
      case r: PandaReplyMessage.Value => r
      case _ => throw new Exception("Return Type Invalid")
    }
  }

  def addNodeLabel(id: Long, label: String): PandaReplyMessage.Value = {
    val res = leaderNodeDriver.addNodeLabel(id, label, leaderNodeEndpointRef, Duration.Inf)
    res match {
      case r: PandaReplyMessage.Value => r
      case _ => throw new Exception("Return Type Invalid")
    }
  }

  def removeNodeLabel(id: Long, label: String): PandaReplyMessage.Value = {
    val res = leaderNodeDriver.removeNodeLabel(id, label, leaderNodeEndpointRef, Duration.Inf)
    res match {
      case r: PandaReplyMessage.Value => r
      case _ => throw new Exception("Return Type Invalid")
    }
  }

  def setNodeProperty(id: Long, propertiesMap: Map[String, Any]): PandaReplyMessage.Value = {
    val res = leaderNodeDriver.setNodeProperty(id: Long, propertiesMap: Map[String, Any], leaderNodeEndpointRef, Duration.Inf)
    res match {
      case r: PandaReplyMessage.Value => r
      case _ => throw new Exception("Return Type Invalid")
    }
  }

  def removeNodeProperty(id: Long, property: String): PandaReplyMessage.Value = {
    val res = leaderNodeDriver.removeNodeProperty(id: Long, property, leaderNodeEndpointRef, Duration.Inf)
    res match {
      case r: PandaReplyMessage.Value => r
      case _ => throw new Exception("Return Type Invalid")
    }
  }

  def createRelationship(nodeId1: Long, nodeId2: Long, relationship: String, direction: Direction.Value): ArrayBuffer[Relationship] = {
    val res = leaderNodeDriver.createNodeRelationship(nodeId1, nodeId2, relationship, direction, leaderNodeEndpointRef, Duration.Inf)
    res match {
      case r: ArrayBuffer[Relationship] => r
      case _ => throw new Exception("Return Type Invalid")
    }
  }

  def setRelationshipProperty(relationId: Long, propertyMap: Map[String, Any]): PandaReplyMessage.Value = {
    val res = leaderNodeDriver.setRelationshipProperty(relationId, propertyMap, leaderNodeEndpointRef, Duration.Inf)
    res match {
      case r: PandaReplyMessage.Value => r
      case _ => throw new Exception("Return Type Invalid")
    }
  }

  def deleteRelationshipProperty(relationId: Long, propertyArray: Array[String]): PandaReplyMessage.Value = {
    val res = leaderNodeDriver.deleteRelationshipProperties(relationId, propertyArray, leaderNodeEndpointRef, Duration.Inf)
    res match {
      case r: PandaReplyMessage.Value => r
      case _ => throw new Exception("Return Type Invalid")
    }
  }

  def deleteNodeRelationship(startNodeId: Long, endNodeId: Long, relationshipName: String, direction: Direction.Value): PandaReplyMessage.Value = {
    val res = leaderNodeDriver.deleteNodeRelationship(startNodeId, endNodeId, relationshipName, direction, leaderNodeEndpointRef, Duration.Inf)
    res match {
      case r: PandaReplyMessage.Value => r
      case _ => throw new Exception("Return Type Invalid")
    }
  }

  def createBlobFromFile(mimeType: MimeType, file: File): BlobEntry = {
    val ins = new FileInputStream(file)
    val length = ins.available()
    val content = new Array[Byte](ins.available())
    ins.read(content)
    leaderNodeDriver.createBlobEntry(length, mimeType, content, leaderNodeEndpointRef, Duration.Inf)
  }

  /// read functions

  def runCypher(cypher: String): InternalRecords = {
    dataNodeDriver.runCypher(cypher, dataNodeEndpointRef, Duration.Inf)
  }

  def getNodeById(id: Long): Node = {
    dataNodeDriver.getNodeById(id, dataNodeEndpointRef, Duration.Inf)
  }

  def findNodes(label: String, propertiesMap: Map[String, Any]): ArrayBuffer[Node] = {
    dataNodeDriver.getNodesByProperty(label, propertiesMap, dataNodeEndpointRef, Duration.Inf)
  }

  def getNodesByLabel(label: String): ArrayBuffer[Node] = {
    dataNodeDriver.getNodesByLabel(label, dataNodeEndpointRef, Duration.Inf)
  }

  def getRelationshipByRelationId(relationId: Long): Unit = {
    dataNodeDriver.getRelationshipByRelationId(relationId, dataNodeEndpointRef, Duration.Inf)
  }

  def getNodeRelationships(nodeId: Long): Unit = {
    dataNodeDriver.getNodeRelationships(nodeId, dataNodeEndpointRef, Duration.Inf)
  }

  def getAllNodes(): Stream[Node] = {
    dataNodeDriver.getAllDBNodes(5, dataNodeEndpointRef, Duration.Inf)
  }

  def getAllRelations(): Unit = {
    dataNodeDriver.getAllDBRelationships(5, dataNodeEndpointRef, Duration.Inf)
  }

  def getAllLabels(): Unit = {
    dataNodeDriver.getAllDBLabels(5, dataNodeEndpointRef, Duration.Inf)
  }

  override def close(): Unit = {
    zkTools.close()
  }
}
