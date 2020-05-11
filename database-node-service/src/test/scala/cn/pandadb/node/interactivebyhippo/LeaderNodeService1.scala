package cn.pandadb.node.interactivebyhippo

import cn.pandadb.configuration.Config
import cn.pandadb.datanode.{DataNodeDriver, SayHello}
import cn.pandadb.util.PandaReplyMsg
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.{HippoEndpointRef, HippoRpcEnvFactory}
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnvClientConfig}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration.Duration


// do cluster data update
trait LeaderNodeService {
  def sayHello(): PandaReplyMsg.Value

  //  def createNode(labels: Array[String], properties: Map[String, Any]): PandaReplyMsg.Value

  //  def addNodeLabel(id: Long, label: String): Int
  //
  //  def getNodeById(id: Long): Int
  //
  //  def getNodesByProperty(label: String, propertiesMap: Map[String, Object]): Int
  //
  //  def getNodesByLabel(label: String): Int
  //
  //  def updateNodeProperty(id: Long, propertiesMap: Map[String, Any]): Int
  //
  //  def updateNodeLabel(id: Long, toDeleteLabel: String, newLabel: String): Int
  //
  //  def deleteNode(id: Long): Int
  //
  //  def removeProperty(id: Long, property: String): Int
  //
  //  def createNodeRelationship(id1: Long, id2: Long, relationship: String, direction: Direction): Int
  //
  //  def getNodeRelationships(id: Long): Int
  //
  //  def deleteNodeRelationship(id: Long, relationship: String, direction: Direction): Int
  //
  //  def getAllDBNodes(chunkSize: Int): Int
  //
  //  def getAllDBRelationships(chunkSize: Int): Int
}


class LeaderNodeServiceImpl1() extends LeaderNodeService {
  val dataNodeDriver = new DataNodeDriver
  val pandaConfig = new Config()

  // leader node services
  override def sayHello(): PandaReplyMsg.Value = {
    // begin cluster transaction
    //TODO: begin leader node's transaction
    val res = sendSayHelloCommandToAllNodes()

    //TODO: close leader node's transaction
    if (res == PandaReplyMsg.LEAD_NODE_SUCCESS) {
      PandaReplyMsg.LEAD_NODE_SUCCESS
    } else {
      PandaReplyMsg.LEAD_NODE_FAILED
    }
  }


  private def sendSayHelloCommandToAllNodes(): PandaReplyMsg.Value = {
    val clientConfig = RpcEnvClientConfig(new RpcConf(), pandaConfig.getRpcServerName())
    val clientRpcEnv = HippoRpcEnvFactory.create(clientConfig)
    val allDataNodeEndpointRefs = ArrayBuffer[HippoEndpointRef]()
    val dataNodes = List("localhost:6666", "localhost:6667")
    dataNodes.map(s => {
      val strs = s.split(":")
      val dataHost = strs(0)
      val dataPort = strs(1).toInt
      val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(dataHost, dataPort), "server")
      allDataNodeEndpointRefs += ref
    })

    val refNumber = allDataNodeEndpointRefs.size
    // send command to all data nodes
    var countReplyRef = 0
    allDataNodeEndpointRefs.par.foreach(endpointRef => {
      val res = Await.result(endpointRef.askWithBuffer[PandaReplyMsg.Value](SayHello("hello")), Duration.Inf)
      if (res == PandaReplyMsg.SUCCESS) {
        countReplyRef += 1
      }
    })
    println("refNumber:", refNumber, "countReply:", countReplyRef)
    clientRpcEnv.shutdown()
    if (countReplyRef == refNumber) {
      PandaReplyMsg.LEAD_NODE_SUCCESS
    } else {
      PandaReplyMsg.LEAD_NODE_FAILED
    }
  }

}
