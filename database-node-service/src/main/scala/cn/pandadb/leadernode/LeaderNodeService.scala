package cn.pandadb.leadernode


import java.util.Random

import cn.pandadb.cluster.ClusterService
import cn.pandadb.configuration.Config
import cn.pandadb.datanode.{DataNodeDriver, SayHello}
import cn.pandadb.driver.result.InternalRecords
import cn.pandadb.driver.values.{Node, Relationship}
import cn.pandadb.util.{PandaReplyMsg, ValueConverter}
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.{HippoEndpointRef, HippoRpcEnv, HippoRpcEnvFactory}
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnvClientConfig}
import org.grapheco.hippo.ChunkedStream
import org.neo4j.graphdb.{Direction, GraphDatabaseService}

import scala.collection.JavaConversions
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration


// do cluster data update
trait LeaderNodeService {
  def sayHello(clusterService: ClusterService): PandaReplyMsg.Value

  def runCypher(cypher: String, clusterService: ClusterService): InternalRecords

  def createNode(labels: Array[String], properties: Map[String, Any], clusterService: ClusterService): PandaReplyMsg.Value

  def deleteNode(id: Long, clusterService: ClusterService): PandaReplyMsg.Value

  def addNodeLabel(id: Long, label: String, clusterService: ClusterService): PandaReplyMsg.Value

  def getNodeById(id: Long, clusterService: ClusterService): Node

  def getNodesByProperty(label: String, propertiesMap: Map[String, Object], clusterService: ClusterService): ArrayBuffer[Node]

  def getNodesByLabel(label: String, clusterService: ClusterService): ArrayBuffer[Node]

  def updateNodeProperty(id: Long, propertiesMap: Map[String, Any], clusterService: ClusterService): PandaReplyMsg.Value

  def updateNodeLabel(id: Long, toDeleteLabel: String, newLabel: String, clusterService: ClusterService): PandaReplyMsg.Value

  def removeProperty(id: Long, property: String, clusterService: ClusterService): PandaReplyMsg.Value

  def createNodeRelationship(id1: Long, id2: Long, relationship: String, direction: Direction, clusterService: ClusterService): PandaReplyMsg.Value

  def getNodeRelationships(id: Long, clusterService: ClusterService): ArrayBuffer[Relationship]

  def deleteNodeRelationship(id: Long, relationship: String, direction: Direction, clusterService: ClusterService): PandaReplyMsg.Value

  //  def getAllDBNodes(localDatabase:GraphDatabaseService, chunkSize: Int): ChunkedStream
  //
  //  def getAllDBRelationships(chunkSize: Int, clusterService: ClusterService): Stream[Node]

  // pull from zk
  def getZkDataNodes(clusterService: ClusterService): List[String]
}


class LeaderNodeServiceImpl() extends LeaderNodeService {
  val dataNodeDriver = new DataNodeDriver
  val config = new Config()
  val clientConfig = RpcEnvClientConfig(new RpcConf(), "panda-client")

  // leader node services


  //  override def getAllDBNodes(localDatabase:GraphDatabaseService, chunkSize: Int): ChunkedStream = {
  //    val tx = localDatabase.beginTx()
  //    val nodesIter = localDatabase.getAllNodes().iterator().stream().iterator()
  //    val iterable = JavaConversions.asScalaIterator(nodesIter).toIterable
  //    ChunkedStream.grouped(chunkSize, iterable.map(x => ValueConverter.toDriverNode(x)), {
  //      tx.success()
  //      tx.close()
  //    })
  //  }

  override def getZkDataNodes(clusterService: ClusterService): List[String] = {
    val res = clusterService.getDataNodes()
    res
  }

  override def deleteNodeRelationship(id: Long, relationship: String, direction: Direction, clusterService: ClusterService): PandaReplyMsg.Value = {
    val (clientRpcEnv, allEndpointRefs) = getAllEndpointRef(clusterService)
    val refNumber = allEndpointRefs.size
    // send command to all data nodes
    var countReplyRef = 0
    allEndpointRefs.par.foreach(endpointRef => {
      val res = dataNodeDriver.deleteNodeRelationship(id, relationship, direction, endpointRef, Duration.Inf)
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

  override def getNodeRelationships(id: Long, clusterService: ClusterService): ArrayBuffer[Relationship] = {
    val clientRpcEnv = HippoRpcEnvFactory.create(clientConfig)
    val dataNodes = clusterService.getDataNodes()
    val nodeNumber = dataNodes.size
    // TODO: load balance
    val chooseNumber = new Random().nextInt(nodeNumber)
    val strs = dataNodes(chooseNumber).split(":")
    val address = strs(0)
    val port = strs(1).toInt
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(address, port), config.getDataNodeEndpointName())
    val res = dataNodeDriver.getNodeRelationships(id, ref, Duration.Inf)
    clientRpcEnv.shutdown()
    res
  }

  override def createNodeRelationship(id1: Long, id2: Long, relationship: String, direction: Direction, clusterService: ClusterService): PandaReplyMsg.Value = {
    val (clientRpcEnv, allEndpointRefs) = getAllEndpointRef(clusterService)
    val refNumber = allEndpointRefs.size
    // send command to all data nodes
    var countReplyRef = 0
    allEndpointRefs.par.foreach(endpointRef => {
      val res = dataNodeDriver.createNodeRelationship(id1, id2, relationship, direction, endpointRef, Duration.Inf)
      if (res == PandaReplyMsg.SUCCESS) {
        countReplyRef += 1
      }
    })
    println(refNumber, countReplyRef)
    clientRpcEnv.shutdown()
    if (countReplyRef == refNumber) {
      PandaReplyMsg.LEAD_NODE_SUCCESS
    } else {
      PandaReplyMsg.LEAD_NODE_FAILED
    }
  }

  override def removeProperty(id: Long, property: String, clusterService: ClusterService): PandaReplyMsg.Value = {
    val (clientRpcEnv, allEndpointRefs) = getAllEndpointRef(clusterService)
    val refNumber = allEndpointRefs.size
    // send command to all data nodes
    var countReplyRef = 0
    allEndpointRefs.par.foreach(endpointRef => {
      val res = dataNodeDriver.removeProperty(id, property, endpointRef, Duration.Inf)
      if (res == PandaReplyMsg.SUCCESS) {
        countReplyRef += 1
      }
    })
    println(refNumber, countReplyRef)
    clientRpcEnv.shutdown()
    if (countReplyRef == refNumber) {
      PandaReplyMsg.LEAD_NODE_SUCCESS
    } else {
      PandaReplyMsg.LEAD_NODE_FAILED
    }
  }

  override def updateNodeLabel(id: Long, toDeleteLabel: String, newLabel: String, clusterService: ClusterService): PandaReplyMsg.Value = {
    val (clientRpcEnv, allEndpointRefs) = getAllEndpointRef(clusterService)
    val refNumber = allEndpointRefs.size
    // send command to all data nodes
    var countReplyRef = 0
    allEndpointRefs.par.foreach(endpointRef => {
      val res = dataNodeDriver.updateNodeLabel(id, toDeleteLabel, newLabel, endpointRef, Duration.Inf)
      if (res == PandaReplyMsg.SUCCESS) {
        countReplyRef += 1
      }
    })
    println(refNumber, countReplyRef)
    clientRpcEnv.shutdown()
    if (countReplyRef == refNumber) {
      PandaReplyMsg.LEAD_NODE_SUCCESS
    } else {
      PandaReplyMsg.LEAD_NODE_FAILED
    }
  }

  override def updateNodeProperty(id: Long, propertiesMap: Map[String, Any], clusterService: ClusterService): PandaReplyMsg.Value = {
    val (clientRpcEnv, allEndpointRefs) = getAllEndpointRef(clusterService)
    val refNumber = allEndpointRefs.size
    // send command to all data nodes
    var countReplyRef = 0
    allEndpointRefs.par.foreach(endpointRef => {
      val res = dataNodeDriver.updateNodeProperty(id, propertiesMap, endpointRef, Duration.Inf)
      if (res == PandaReplyMsg.SUCCESS) {
        countReplyRef += 1
      }
    })
    println(refNumber, countReplyRef)
    clientRpcEnv.shutdown()
    if (countReplyRef == refNumber) {
      PandaReplyMsg.LEAD_NODE_SUCCESS
    } else {
      PandaReplyMsg.LEAD_NODE_FAILED
    }
  }

  override def getNodesByLabel(label: String, clusterService: ClusterService): ArrayBuffer[Node] = {
    val clientRpcEnv = HippoRpcEnvFactory.create(clientConfig)
    val dataNodes = clusterService.getDataNodes()
    val nodeNumber = dataNodes.size
    // TODO: load balance
    val chooseNumber = new Random().nextInt(nodeNumber)
    val strs = dataNodes(chooseNumber).split(":")
    val address = strs(0)
    val port = strs(1).toInt
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(address, port), config.getDataNodeEndpointName())
    val res = dataNodeDriver.getNodesByLabel(label, ref, Duration.Inf)
    clientRpcEnv.shutdown()
    res
  }

  override def getNodesByProperty(label: String, propertiesMap: Map[String, Object], clusterService: ClusterService): ArrayBuffer[Node] = {
    val clientRpcEnv = HippoRpcEnvFactory.create(clientConfig)
    val dataNodes = clusterService.getDataNodes()
    val nodeNumber = dataNodes.size
    // TODO: load balance
    val chooseNumber = new Random().nextInt(nodeNumber)
    val strs = dataNodes(chooseNumber).split(":")
    val address = strs(0)
    val port = strs(1).toInt
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(address, port), config.getDataNodeEndpointName())
    val res = dataNodeDriver.getNodesByProperty(label, propertiesMap, ref, Duration.Inf)
    clientRpcEnv.shutdown()
    res
  }

  override def sayHello(clusterService: ClusterService): PandaReplyMsg.Value = {
    // begin cluster transaction
    //TODO: begin leader node's transaction
    val res = sendSayHelloCommandToAllNodes(clusterService)

    //TODO: close leader node's transaction
    if (res == PandaReplyMsg.LEAD_NODE_SUCCESS) {
      PandaReplyMsg.LEAD_NODE_SUCCESS
    } else {
      PandaReplyMsg.LEAD_NODE_FAILED
    }
  }

  override def runCypher(cypher: String, clusterService: ClusterService): InternalRecords = {
    val clientRpcEnv = HippoRpcEnvFactory.create(clientConfig)
    val dataNodes = clusterService.getDataNodes()
    val nodeNumber = dataNodes.size
    // TODO: load balance
    val chooseNumber = new Random().nextInt(nodeNumber)
    val strs = dataNodes(chooseNumber).split(":")
    val address = strs(0)
    val port = strs(1).toInt
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(address, port), config.getDataNodeEndpointName())
    val res = dataNodeDriver.runCypher(cypher, ref, Duration.Inf)
    clientRpcEnv.shutdown()
    res
  }

  override def createNode(labels: Array[String], properties: Map[String, Any], clusterService: ClusterService): PandaReplyMsg.Value = {
    val (clientRpcEnv, allEndpointRefs) = getAllEndpointRef(clusterService)
    val refNumber = allEndpointRefs.size
    // send command to all data nodes
    var countReplyRef = 0
    allEndpointRefs.par.foreach(endpointRef => {
      val res = dataNodeDriver.createNode(labels, properties, endpointRef, Duration.Inf)
      if (res.isInstanceOf[Node]) {
        countReplyRef += 1
      }
    })
    println(refNumber, countReplyRef)
    clientRpcEnv.shutdown()
    if (countReplyRef == refNumber) {
      PandaReplyMsg.LEAD_NODE_SUCCESS
    } else {
      PandaReplyMsg.LEAD_NODE_FAILED
    }
  }

  override def deleteNode(id: Long, clusterService: ClusterService): PandaReplyMsg.Value = {
    val (clientRpcEnv, allEndpointRefs) = getAllEndpointRef(clusterService)
    val refNumber = allEndpointRefs.size
    // send command to all data nodes
    var countReplyRef = 0
    allEndpointRefs.par.foreach(endpointRef => {
      val res = dataNodeDriver.deleteNode(id, endpointRef, Duration.Inf)
      if (res == PandaReplyMsg.SUCCESS) {
        countReplyRef += 1
      }
    })
    println(refNumber, countReplyRef)
    clientRpcEnv.shutdown()
    if (countReplyRef == refNumber) {
      PandaReplyMsg.LEAD_NODE_SUCCESS
    } else {
      PandaReplyMsg.LEAD_NODE_FAILED
    }
  }

  override def addNodeLabel(id: Long, label: String, clusterService: ClusterService): PandaReplyMsg.Value = {
    val (clientRpcEnv, allEndpointRefs) = getAllEndpointRef(clusterService)
    val refNumber = allEndpointRefs.size
    // send command to all data nodes
    var countReplyRef = 0
    allEndpointRefs.par.foreach(endpointRef => {
      val res = dataNodeDriver.addNodeLabel(id, label, endpointRef, Duration.Inf)
      if (res == PandaReplyMsg.SUCCESS) {
        countReplyRef += 1
      }
    })
    println(refNumber, countReplyRef)
    clientRpcEnv.shutdown()
    if (countReplyRef == refNumber) {
      PandaReplyMsg.LEAD_NODE_SUCCESS
    } else {
      PandaReplyMsg.LEAD_NODE_FAILED
    }
  }


  override def getNodeById(id: Long, clusterService: ClusterService): Node = {
    val clientRpcEnv = HippoRpcEnvFactory.create(clientConfig)
    val dataNodes = clusterService.getDataNodes()
    val nodeNumber = dataNodes.size
    // TODO: load balance
    val chooseNumber = new Random().nextInt(nodeNumber)
    val strs = dataNodes(chooseNumber).split(":")
    val address = strs(0)
    val port = strs(1).toInt
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(address, port), config.getDataNodeEndpointName())
    val res = dataNodeDriver.getNodeById(id, ref, Duration.Inf)
    clientRpcEnv.shutdown()
    res
  }

  private def sendSayHelloCommandToAllNodes(clusterService: ClusterService): PandaReplyMsg.Value = {
    val (clientRpcEnv, allEndpointRefs) = getAllEndpointRef(clusterService)
    val refNumber = allEndpointRefs.size

    // send command to all data nodes
    var countReplyRef = 0
    allEndpointRefs.par.foreach(endpointRef => {
      val res = dataNodeDriver.sayHello("hello", endpointRef, Duration.Inf)
      if (res == PandaReplyMsg.SUCCESS) {
        countReplyRef += 1
      }
    })
    println(refNumber, countReplyRef)
    clientRpcEnv.shutdown()
    if (countReplyRef == refNumber) {
      PandaReplyMsg.LEAD_NODE_SUCCESS
    } else {
      PandaReplyMsg.LEAD_NODE_FAILED
    }
  }

  def getAllEndpointRef(clusterService: ClusterService): (HippoRpcEnv, ArrayBuffer[HippoEndpointRef]) = {
    val dataNodes = clusterService.getDataNodes()
    val clientRpcEnv = HippoRpcEnvFactory.create(clientConfig)
    val allEndpointRefs = ArrayBuffer[HippoEndpointRef]()
    dataNodes.map(s => {
      val strs = s.split(":")
      val address = strs(0)
      val port = strs(1).toInt
      val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(address, port), config.getDataNodeEndpointName())
      allEndpointRefs += ref
    })
    (clientRpcEnv, allEndpointRefs)
  }

  def test(driverFunc: DataNodeDriver => Any): Unit = {

  }
}
