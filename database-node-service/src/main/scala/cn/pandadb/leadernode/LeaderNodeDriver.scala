package cn.pandadb.leadernode

import java.nio.ByteBuffer
import java.util.Random

import cn.pandadb.blob.{BlobEntry, MimeType}
import cn.pandadb.cluster.ClusterService
import cn.pandadb.configuration.Config
import cn.pandadb.datanode.DataNodeDriver
import cn.pandadb.driver.result.InternalRecords
import cn.pandadb.driver.values.{Direction, Label, Node, Relationship}
import cn.pandadb.util.PandaReplyMessage
import io.netty.buffer.{ByteBuf, Unpooled}
import net.neoremind.kraps.rpc.RpcAddress
import net.neoremind.kraps.rpc.netty.{HippoEndpointRef, HippoRpcEnv}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class LeaderNodeDriver {
  val config = new Config
  val logger = config.getLogger(this.getClass)

  def getZkDataNodes(endpointRef: HippoEndpointRef, duration: Duration): List[String] = {
    val res = Await.result(endpointRef.askWithBuffer[List[String]](GetZkDataNodes()), duration)
    res
  }

  def getDbFileNames(endpointRef: HippoEndpointRef, duration: Duration): ArrayBuffer[String] = {
    val res = Await.result(endpointRef.askWithBuffer[ArrayBuffer[String]](GetLeaderDbFileNames()), duration)
    res
  }

  def sayHello(msg: String, endpointRef: HippoEndpointRef, duration: Duration): PandaReplyMessage.Value = {
    val res = Await.result(endpointRef.askWithBuffer[PandaReplyMessage.Value](LeaderSayHello(msg)), duration)
    res
  }

  def runCypher(cypher: String, endpointRef: HippoEndpointRef, duration: Duration): InternalRecords = {
    val res = Await.result(endpointRef.askWithBuffer[InternalRecords](LeaderRunCypher(cypher)), duration)
    res
  }

  def createNode(labels: Array[String], properties: Map[String, Any], endpointRef: HippoEndpointRef, duration: Duration): Any = {
    val res = Await.result(endpointRef.askWithBuffer[Any](LeaderCreateNode(labels, properties)), duration)
    res match {
      case n: Node => res.asInstanceOf[Node]
      case p: PandaReplyMessage.Value => res.asInstanceOf[PandaReplyMessage.Value]
    }
  }

  def createNodeRelationship(id1: Long, id2: Long, relationship: String, direction: Direction.Value, endpointRef: HippoEndpointRef, duration: Duration): Any = {
    val res = Await.result(endpointRef.askWithBuffer[Any](LeaderCreateNodeRelationship(id1, id2, relationship, direction)), duration)
    res match {
      case r: ArrayBuffer[Relationship] => res.asInstanceOf[ArrayBuffer[Relationship]]
      case p: PandaReplyMessage.Value => res.asInstanceOf[PandaReplyMessage.Value]
    }
  }

  def addNodeLabel(id: Long, label: String, endpointRef: HippoEndpointRef, duration: Duration): PandaReplyMessage.Value = {
    val res = Await.result(endpointRef.askWithBuffer[PandaReplyMessage.Value](LeaderAddNodeLabel(id, label)), duration)
    res
  }

  def getNodeById(id: Long, endpointRef: HippoEndpointRef, duration: Duration): Node = {
    val node = Await.result(endpointRef.askWithBuffer[Node](LeaderGetNodeById(id)), duration)
    node
  }

  def getNodesByProperty(label: String, propertiesMap: Map[String, Any], endpointRef: HippoEndpointRef, duration: Duration): ArrayBuffer[Node] = {
    implicit def any2Object(x: Map[String, Any]): Map[String, Object] = x.asInstanceOf[Map[String, Object]]

    val res = Await.result(endpointRef.askWithBuffer[ArrayBuffer[Node]](LeaderGetNodesByProperty(label, propertiesMap)), duration)
    res
  }

  def getNodesByLabel(label: String, endpointRef: HippoEndpointRef, duration: Duration): ArrayBuffer[Node] = {
    val res = Await.result(endpointRef.askWithBuffer[ArrayBuffer[Node]](LeaderGetNodesByLabel(label)), duration)
    res
  }

  def setNodeProperty(id: Long, propertiesMap: Map[String, Any], endpointRef: HippoEndpointRef, duration: Duration): PandaReplyMessage.Value = {
    val res = Await.result(endpointRef.askWithBuffer[PandaReplyMessage.Value](LeaderSetNodeProperty(id, propertiesMap)), duration)
    res
  }

  def removeNodeLabel(id: Long, toDeleteLabel: String, endpointRef: HippoEndpointRef, duration: Duration): PandaReplyMessage.Value = {
    val res = Await.result(endpointRef.askWithBuffer[PandaReplyMessage.Value](LeaderRemoveNodeLabel(id, toDeleteLabel)), duration)
    res
  }

  def deleteNode(id: Long, endpointRef: HippoEndpointRef, duration: Duration): PandaReplyMessage.Value = {
    val res = Await.result(endpointRef.askWithBuffer[PandaReplyMessage.Value](LeaderDeleteNode(id)), duration)
    res
  }

  def removeNodeProperty(id: Long, property: String, endpointRef: HippoEndpointRef, duration: Duration): PandaReplyMessage.Value = {
    val res = Await.result(endpointRef.askWithBuffer[PandaReplyMessage.Value](LeaderRemoveNodeProperty(id, property)), duration)
    res
  }

  def getRelationshipByRelationId(relationId: Long, endpointRef: HippoEndpointRef, duration: Duration): Relationship = {
    val relation = Await.result(endpointRef.askWithBuffer[Relationship](LeaderGetRelationshipByRelationId(relationId)), duration)
    relation
  }

  def setRelationshipProperty(relationId: Long, propertyMap: Map[String, Any], endpointRef: HippoEndpointRef, duration: Duration): PandaReplyMessage.Value = {
    implicit def any2anyRef(x: Map[String, Any]): Map[String, AnyRef] = x.asInstanceOf[Map[String, AnyRef]]

    val res = Await.result(endpointRef.askWithBuffer[PandaReplyMessage.Value](LeaderSetRelationshipProperty(relationId, propertyMap)), duration)
    res
  }

  def deleteRelationshipProperties(relationId: Long, propertyArray: Array[String], endpointRef: HippoEndpointRef, duration: Duration): PandaReplyMessage.Value = {
    val res = Await.result(endpointRef.askWithBuffer[PandaReplyMessage.Value](LeaderDeleteRelationshipProperties(relationId, propertyArray)), duration)
    res
  }

  def getNodeRelationships(nodeId: Long, endpointRef: HippoEndpointRef, duration: Duration): ArrayBuffer[Relationship] = {
    val res = Await.result(endpointRef.askWithBuffer[ArrayBuffer[Relationship]](LeaderGetNodeRelationships(nodeId)), duration)
    res
  }

  def deleteNodeRelationship(startNodeId: Long, endNodeId: Long, relationshipName: String, direction: Direction.Value, endpointRef: HippoEndpointRef, duration: Duration): PandaReplyMessage.Value = {
    val res = Await.result(endpointRef.askWithBuffer[PandaReplyMessage.Value](LeaderDeleteNodeRelationship(startNodeId, endNodeId, relationshipName, direction)), duration)
    res
  }

  //  def getAllDBRelationships(chunkSize: Int, endpointRef: HippoEndpointRef, duration: Duration): Stream[Relationship] = {
  //    val res = endpointRef.getChunkedStream[Relationship](LeaderGetAllDBRelationships(chunkSize), duration)
  //    res
  //  }

  def pullAllNodes(chunkSize: Int, clientRpcEnv: HippoRpcEnv, clusterService: ClusterService, config: Config): (Iterator[Node], HippoEndpointRef) = {
    val dataNodeDriver = new DataNodeDriver
    val str = clusterService.getLeaderNode().split(":")
    val addr = str(0)
    val port = str(1).toInt
    val leaderRef = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val nodes = getZkDataNodes(leaderRef, Duration.Inf)
    clientRpcEnv.stop(leaderRef)

    val choose = nodes(new Random().nextInt(nodes.size))
    val strs = choose.split(":")
    val address2 = strs(0)
    val port2 = strs(1).toInt
    val dataNodeRef = clientRpcEnv.setupEndpointRef(new RpcAddress(address2, port2), config.getDataNodeEndpointName())
    val res = dataNodeDriver.getAllDBNodes(chunkSize, dataNodeRef, Duration.Inf).iterator
    (res, dataNodeRef)
  }

  def pullAllRelations(chunkSize: Int, clientRpcEnv: HippoRpcEnv, clusterService: ClusterService, config: Config): (Iterator[Relationship], HippoEndpointRef) = {
    val dataNodeDriver = new DataNodeDriver
    val str = clusterService.getLeaderNode().split(":")
    val addr = str(0)
    val port = str(1).toInt
    val leaderRef = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val nodes = getZkDataNodes(leaderRef, Duration.Inf)
    clientRpcEnv.stop(leaderRef)

    val choose = nodes(new Random().nextInt(nodes.size))
    val strs = choose.split(":")
    val address2 = strs(0)
    val port2 = strs(1).toInt
    val dataNodeRef = clientRpcEnv.setupEndpointRef(new RpcAddress(address2, port2), config.getDataNodeEndpointName())
    val res = dataNodeDriver.getAllDBRelationships(chunkSize, dataNodeRef, Duration.Inf).iterator
    (res, dataNodeRef)
  }

  def pullAllLabels(chunkSize: Int, clientRpcEnv: HippoRpcEnv, clusterService: ClusterService, config: Config): (Iterator[Label], HippoEndpointRef) = {
    val dataNodeDriver = new DataNodeDriver
    val str = clusterService.getLeaderNode().split(":")
    val addr = str(0)
    val port = str(1).toInt
    val leaderRef = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val nodes = getZkDataNodes(leaderRef, Duration.Inf)
    clientRpcEnv.stop(leaderRef)

    val choose = nodes(new Random().nextInt(nodes.size))
    val strs = choose.split(":")
    val address2 = strs(0)
    val port2 = strs(1).toInt
    val dataNodeRef = clientRpcEnv.setupEndpointRef(new RpcAddress(address2, port2), config.getDataNodeEndpointName())
    val res = dataNodeDriver.getAllDBLabels(chunkSize, dataNodeRef, Duration.Inf).iterator
    (res, dataNodeRef)
  }

  def pullDbFileFromDataNode(toPath: String, clientRpcEnv: HippoRpcEnv, clusterService: ClusterService, config: Config): HippoEndpointRef = {
    val str = clusterService.getLeaderNode().split(":")
    val addr = str(0)
    val port = str(1).toInt
    val leaderRef = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val nodes = getZkDataNodes(leaderRef, Duration.Inf)
    val fileNames = getDbFileNames(leaderRef, Duration.Inf)
    clientRpcEnv.stop(leaderRef)
    val chooseNode = nodes(new Random().nextInt(nodes.size))

    val strs = chooseNode.split(":")
    val address2 = strs(0)
    val port2 = strs(1).toInt
    val dataNodeRef = clientRpcEnv.setupEndpointRef(new RpcAddress(address2, port2), config.getDataNodeEndpointName())
    val dataNodeDriver = new DataNodeDriver
    val res = dataNodeDriver.pullFile(toPath, fileNames, dataNodeRef, Duration.Inf)
    logger.info(s"+++++++++pull file from DB: $res+++++++++++++")
    dataNodeRef
  }


  def createBlobEntry(length: Long, mimeType: MimeType, content: Array[Byte], endpointRef: HippoEndpointRef, duration: Duration): BlobEntry = {
    println(content)
    val res = Await.result(endpointRef.askWithBuffer[BlobEntry](LeaderCreateBlobEntry(length, mimeType), Unpooled.copiedBuffer(content)), duration)
    res
  }
}
