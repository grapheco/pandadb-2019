package cn.pandadb.leadernode

import cn.pandadb.driver.values.{Node, Relationship}
import cn.pandadb.util.PandaReplyMsg
import net.neoremind.kraps.rpc.netty.HippoEndpointRef
import org.neo4j.graphdb.Direction

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class LeaderNodeDriver {
  def sayHello(msg: String, endpointRef: HippoEndpointRef, duration: Duration): PandaReplyMsg.Value = {
    val res = Await.result(endpointRef.ask[PandaReplyMsg.Value](LeaderSayHello(msg)), duration)
    res
  }

  def createNode(labels: Array[String], properties: Map[String, Any], endpointRef: HippoEndpointRef, duration: Duration): Node = {
    val node = Await.result(endpointRef.ask[Node](LeaderCreateNode(labels, properties)), duration)
    node
  }

  def addNodeLabel(id: Long, label: String, endpointRef: HippoEndpointRef, duration: Duration): PandaReplyMsg.Value = {
    val res = Await.result(endpointRef.ask[PandaReplyMsg.Value](LeaderAddNodeLabel(id, label)), duration)
    res
  }

  def getNodeById(id: Long, endpointRef: HippoEndpointRef, duration: Duration): Node = {
    val node = Await.result(endpointRef.ask[Node](LeaderGetNodeById(id)), duration)
    node
  }

  def getNodesByProperty(label: String, propertiesMap: Map[String, Object], endpointRef: HippoEndpointRef, duration: Duration): ArrayBuffer[Node] = {
    implicit def any2Object(x: Map[String, Any]): Map[String, Object] = x.asInstanceOf[Map[String, Object]]

    val res = Await.result(endpointRef.ask[ArrayBuffer[Node]](LeaderGetNodesByProperty(label, propertiesMap)), duration)
    res
  }

  def getNodesByLabel(label: String, endpointRef: HippoEndpointRef, duration: Duration): ArrayBuffer[Node] = {
    val res = Await.result(endpointRef.ask[ArrayBuffer[Node]](LeaderGetNodesByLabel(label)), duration)
    res
  }

  def updateNodeProperty(id: Long, propertiesMap: Map[String, Any], endpointRef: HippoEndpointRef, duration: Duration): PandaReplyMsg.Value = {
    val res = Await.result(endpointRef.ask[PandaReplyMsg.Value](LeaderUpdateNodeProperty(id, propertiesMap)), duration)
    res
  }

  def updateNodeLabel(id: Long, toDeleteLabel: String, newLabel: String, endpointRef: HippoEndpointRef, duration: Duration): PandaReplyMsg.Value = {
    val res = Await.result(endpointRef.ask[PandaReplyMsg.Value](LeaderUpdateNodeLabel(id, toDeleteLabel, newLabel)), duration)
    res
  }

  def deleteNode(id: Long, endpointRef: HippoEndpointRef, duration: Duration): PandaReplyMsg.Value = {
    val res = Await.result(endpointRef.ask[PandaReplyMsg.Value](LeaderDeleteNode(id)), duration)
    res
  }

  def removeProperty(id: Long, property: String, endpointRef: HippoEndpointRef, duration: Duration): PandaReplyMsg.Value = {
    val res = Await.result(endpointRef.ask[PandaReplyMsg.Value](LeaderRemoveProperty(id, property)), duration)
    res
  }

  def createNodeRelationship(id1: Long, id2: Long, relationship: String, direction: Direction, endpointRef: HippoEndpointRef, duration: Duration): PandaReplyMsg.Value = {
    val res = Await.result(endpointRef.ask[PandaReplyMsg.Value](LeaderCreateNodeRelationship(id1, id2, relationship, Direction.OUTGOING)), duration)
    res
  }

  def getNodeRelationships(id: Long, endpointRef: HippoEndpointRef, duration: Duration): ArrayBuffer[Relationship] = {
    val res = Await.result(endpointRef.ask[ArrayBuffer[Relationship]](LeaderGetNodeRelationships(id)), duration)
    res
  }

  def deleteNodeRelationship(id: Long, relationship: String, direction: Direction, endpointRef: HippoEndpointRef, duration: Duration): PandaReplyMsg.Value = {
    val res = Await.result(endpointRef.ask[PandaReplyMsg.Value](LeaderDeleteNodeRelationship(id, relationship, Direction.OUTGOING)), duration)
    res
  }

  //  def getAllDBNodes(chunkSize: Int, endpointRef: HippoEndpointRef, duration: Duration): Stream[Node] = {
  //    val res = endpointRef.getChunkedStream[Node](LeaderGetAllDBNodes(chunkSize), duration)
  //    res
  //  }
  //
  //  def getAllDBRelationships(chunkSize: Int, endpointRef: HippoEndpointRef, duration: Duration): Stream[Relationship] = {
  //    val res = endpointRef.getChunkedStream[Relationship](LeaderGetAllDBRelationships(chunkSize), duration)
  //    res
  //  }
}
