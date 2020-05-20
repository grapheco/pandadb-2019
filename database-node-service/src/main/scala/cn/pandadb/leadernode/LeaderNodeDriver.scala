package cn.pandadb.leadernode

import java.util.Random

import cn.pandadb.cluster.ClusterService
import cn.pandadb.configuration.Config
import cn.pandadb.datanode.DataNodeDriver
import cn.pandadb.driver.result.InternalRecords
import cn.pandadb.driver.values.{Node, Relationship}
import cn.pandadb.util.PandaReplyMessage
import net.neoremind.kraps.rpc.RpcAddress
import net.neoremind.kraps.rpc.netty.{HippoEndpointRef, HippoRpcEnv}
import org.neo4j.graphdb.Direction

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class LeaderNodeDriver {

  def getZkDataNodes(endpointRef: HippoEndpointRef, duration: Duration): List[String] = {
    val res = Await.result(endpointRef.askWithBuffer[List[String]](GetZkDataNodes()), duration)
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

  //  def runCypherOnAllNodes(cypher: String, endpointRef: HippoEndpointRef, duration: Duration): ArrayBuffer[InternalRecords] = {
  //    val res = Await.result(endpointRef.askWithBuffer[ArrayBuffer[InternalRecords]](LeaderRunCypherOnAllNodes(cypher)), duration)
  //    res
  //  }

  def createNode(labels: Array[String], properties: Map[String, Any], endpointRef: HippoEndpointRef, duration: Duration): PandaReplyMessage.Value = {
    val res = Await.result(endpointRef.askWithBuffer[PandaReplyMessage.Value](LeaderCreateNode(labels, properties)), duration)
    res
  }

  def addNodeLabel(id: Long, label: String, endpointRef: HippoEndpointRef, duration: Duration): PandaReplyMessage.Value = {
    val res = Await.result(endpointRef.askWithBuffer[PandaReplyMessage.Value](LeaderAddNodeLabel(id, label)), duration)
    res
  }

  def getNodeById(id: Long, endpointRef: HippoEndpointRef, duration: Duration): Node = {
    val node = Await.result(endpointRef.askWithBuffer[Node](LeaderGetNodeById(id)), duration)
    node
  }

  def getNodesByProperty(label: String, propertiesMap: Map[String, Object], endpointRef: HippoEndpointRef, duration: Duration): ArrayBuffer[Node] = {
    implicit def any2Object(x: Map[String, Any]): Map[String, Object] = x.asInstanceOf[Map[String, Object]]

    val res = Await.result(endpointRef.askWithBuffer[ArrayBuffer[Node]](LeaderGetNodesByProperty(label, propertiesMap)), duration)
    res
  }

  def getNodesByLabel(label: String, endpointRef: HippoEndpointRef, duration: Duration): ArrayBuffer[Node] = {
    val res = Await.result(endpointRef.askWithBuffer[ArrayBuffer[Node]](LeaderGetNodesByLabel(label)), duration)
    res
  }

  def updateNodeProperty(id: Long, propertiesMap: Map[String, Any], endpointRef: HippoEndpointRef, duration: Duration): PandaReplyMessage.Value = {
    val res = Await.result(endpointRef.askWithBuffer[PandaReplyMessage.Value](LeaderUpdateNodeProperty(id, propertiesMap)), duration)
    res
  }

  def updateNodeLabel(id: Long, toDeleteLabel: String, newLabel: String, endpointRef: HippoEndpointRef, duration: Duration): PandaReplyMessage.Value = {
    val res = Await.result(endpointRef.askWithBuffer[PandaReplyMessage.Value](LeaderUpdateNodeLabel(id, toDeleteLabel, newLabel)), duration)
    res
  }

  def deleteNode(id: Long, endpointRef: HippoEndpointRef, duration: Duration): PandaReplyMessage.Value = {
    val res = Await.result(endpointRef.askWithBuffer[PandaReplyMessage.Value](LeaderDeleteNode(id)), duration)
    res
  }

  def removeProperty(id: Long, property: String, endpointRef: HippoEndpointRef, duration: Duration): PandaReplyMessage.Value = {
    val res = Await.result(endpointRef.askWithBuffer[PandaReplyMessage.Value](LeaderRemoveProperty(id, property)), duration)
    res
  }

  def createNodeRelationship(id1: Long, id2: Long, relationship: String, direction: Direction, endpointRef: HippoEndpointRef, duration: Duration): PandaReplyMessage.Value = {
    val res = Await.result(endpointRef.askWithBuffer[PandaReplyMessage.Value](LeaderCreateNodeRelationship(id1, id2, relationship, direction)), duration)
    res
  }

  def getNodeRelationships(id: Long, endpointRef: HippoEndpointRef, duration: Duration): ArrayBuffer[Relationship] = {
    val res = Await.result(endpointRef.askWithBuffer[ArrayBuffer[Relationship]](LeaderGetNodeRelationships(id)), duration)
    res
  }

  def deleteNodeRelationship(id: Long, relationship: String, direction: Direction, endpointRef: HippoEndpointRef, duration: Duration): PandaReplyMessage.Value = {
    val res = Await.result(endpointRef.askWithBuffer[PandaReplyMessage.Value](LeaderDeleteNodeRelationship(id, relationship, direction)), duration)
    res
  }

  def getAllDBRelationships(chunkSize: Int, endpointRef: HippoEndpointRef, duration: Duration): Stream[Relationship] = {
    val res = endpointRef.getChunkedStream[Relationship](LeaderGetAllDBRelationships(chunkSize), duration)
    res
  }

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
}
