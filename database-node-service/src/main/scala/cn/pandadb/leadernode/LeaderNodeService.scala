package cn.pandadb.leadernode

import cn.pandadb.datanode.{DataNodeDriver, SayHello}
import cn.pandadb.util.PandaReplyMsg
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.{HippoEndpointRef, HippoRpcEnv, HippoRpcEnvFactory}
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnvClientConfig}

import scala.collection.mutable.ArrayBuffer
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


class LeaderNodeServiceImpl() extends LeaderNodeService {
  val dataNodeDriver = new DataNodeDriver

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
    // TODO:get all data nodes address/port from zookeeper
    val address1 = "localhost"
    val port1 = 6666
    val name1 = "server1"
    val address2 = "localhost"
    val port2 = 6667
    val name2 = "server2"
    val dataNodesMap = Map(
      "node1" -> Map("address" -> address1, "port" -> port1, "name" -> name1),
      "node2" -> Map("address" -> address2, "port" -> port2, "name" -> name2)
    )

    val clientConfig = RpcEnvClientConfig(new RpcConf(), "panda-client")
    val clientRpcEnv = HippoRpcEnvFactory.create(clientConfig)
    val allEndpointRefs = getEndpointRefs(clientRpcEnv, dataNodesMap)
    val refNumber = allEndpointRefs.size

    // send command to all data nodes
    var countReplyRef = 0
    allEndpointRefs.par.foreach(endpointRef => {
      val res = dataNodeDriver.sayHello("hello", endpointRef, Duration.Inf)
      if (res == PandaReplyMsg.SUCCESS) {
        countReplyRef += 1
      }
    })
    clientRpcEnv.shutdown()
    if (countReplyRef == refNumber) {
      PandaReplyMsg.LEAD_NODE_SUCCESS
    } else {
      PandaReplyMsg.LEAD_NODE_FAILED
    }
  }

  def getEndpointRefs(clientRpcEnv: HippoRpcEnv, dataNodesMap: Map[String, Map[String, Any]]): ArrayBuffer[HippoEndpointRef] = {
    val lst = ArrayBuffer[HippoEndpointRef]()
    dataNodesMap.map(map => {
      val endpointRef = clientRpcEnv.setupEndpointRef(
        new RpcAddress(map._2.apply("address").asInstanceOf[String], map._2.apply("port").asInstanceOf[Int]),
        map._2.apply("name").asInstanceOf[String])
      lst += endpointRef
    })
    lst
  }
}
