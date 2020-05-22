package cn.pandadb.leadernode

import java.util.Random

import cn.pandadb.cluster.ClusterService
import cn.pandadb.configuration.Config
import cn.pandadb.datanode.{DataNodeDriver, SayHello}
import cn.pandadb.driver.result.InternalRecords
import cn.pandadb.driver.values.{Node, Relationship}
import cn.pandadb.util.{PandaReplyMessage, ValueConverter}
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
  def sayHello(clusterService: ClusterService): PandaReplyMessage.Value

  def runCypher(cypher: String, address: String, port: Int, clusterService: ClusterService): InternalRecords

  def createNode(id: Long, labels: Array[String], properties: Map[String, Any], clusterService: ClusterService): PandaReplyMessage.Value

  def deleteNode(id: Long, clusterService: ClusterService): PandaReplyMessage.Value

  def addNodeLabel(id: Long, label: String, clusterService: ClusterService): PandaReplyMessage.Value

  def getNodeById(id: Long, address: String, port: Int, clusterService: ClusterService): Node

  def getNodesByProperty(label: String, address: String, port: Int, propertiesMap: Map[String, Object], clusterService: ClusterService): ArrayBuffer[Node]

  def getNodesByLabel(label: String, address: String, port: Int, clusterService: ClusterService): ArrayBuffer[Node]

  def updateNodeProperty(id: Long, propertiesMap: Map[String, Any], clusterService: ClusterService): PandaReplyMessage.Value

  def updateNodeLabel(id: Long, toDeleteLabel: String, newLabel: String, clusterService: ClusterService): PandaReplyMessage.Value

  def removeProperty(id: Long, property: String, clusterService: ClusterService): PandaReplyMessage.Value

  def createNodeRelationship(id1: Long, id2: Long, relationship: String, direction: Direction, clusterService: ClusterService): PandaReplyMessage.Value

  def getNodeRelationships(id: Long, address: String, port: Int, clusterService: ClusterService): ArrayBuffer[Relationship]

  def deleteNodeRelationship(id: Long, relationship: String, direction: Direction, clusterService: ClusterService): PandaReplyMessage.Value

  def getRelationshipByRelationId(id: Long, address: String, port: Int, clusterService: ClusterService): Relationship

  def updateRelationshipProperty(id: Long, propertyMap: Map[String, AnyRef], clusterService: ClusterService): PandaReplyMessage.Value

  def deleteRelationshipProperties(id: Long, propertyArray: Array[String], clusterService: ClusterService): PandaReplyMessage.Value

  // pull from zk
  def getZkDataNodes(clusterService: ClusterService): List[String]
}


class LeaderNodeServiceImpl() extends LeaderNodeService {
  val dataNodeDriver = new DataNodeDriver
  val config = new Config()
  val clientConfig = RpcEnvClientConfig(new RpcConf(), "panda-client")


  override def getRelationshipByRelationId(id: Long, address: String, port: Int, clusterService: ClusterService): Relationship = {
    val clientRpcEnv = HippoRpcEnvFactory.create(clientConfig)
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(address, port), config.getDataNodeEndpointName())
    val res = dataNodeDriver.getRelationshipByRelationId(id, ref, Duration.Inf)
    clientRpcEnv.shutdown()
    res
  }

  override def updateRelationshipProperty(id: Long, propertyMap: Map[String, AnyRef], clusterService: ClusterService): PandaReplyMessage.Value = {
    val (clientRpcEnv, allEndpointRefs) = getEndpointRefsNotIncludeLeader(clusterService)
    val refNumber = allEndpointRefs.size
    // send command to all data nodes
    var countReplyRef = 0
    allEndpointRefs.par.foreach(endpointRef => {
      val res = dataNodeDriver.updateRelationshipProperty(id, propertyMap, endpointRef, Duration.Inf)
      if (res == PandaReplyMessage.SUCCESS) {
        countReplyRef += 1
      }
      clientRpcEnv.stop(endpointRef)
    })
    clientRpcEnv.shutdown()
    if (countReplyRef == refNumber) {
      PandaReplyMessage.LEAD_NODE_SUCCESS
    } else {
      PandaReplyMessage.LEAD_NODE_FAILED
    }
  }

  override def deleteRelationshipProperties(id: Long, propertyArray: Array[String], clusterService: ClusterService): PandaReplyMessage.Value = {
    val (clientRpcEnv, allEndpointRefs) = getEndpointRefsNotIncludeLeader(clusterService)
    val refNumber = allEndpointRefs.size
    // send command to all data nodes
    var countReplyRef = 0
    allEndpointRefs.par.foreach(endpointRef => {
      val res = dataNodeDriver.deleteRelationshipProperties(id, propertyArray, endpointRef, Duration.Inf)
      if (res == PandaReplyMessage.SUCCESS) {
        countReplyRef += 1
      }
      clientRpcEnv.stop(endpointRef)
    })
    clientRpcEnv.shutdown()
    if (countReplyRef == refNumber) {
      PandaReplyMessage.LEAD_NODE_SUCCESS
    } else {
      PandaReplyMessage.LEAD_NODE_FAILED
    }
  }

  override def getZkDataNodes(clusterService: ClusterService): List[String] = {
    val res = clusterService.getDataNodes()
    res
  }

  override def deleteNodeRelationship(id: Long, relationship: String, direction: Direction, clusterService: ClusterService): PandaReplyMessage.Value = {
    val (clientRpcEnv, allEndpointRefs) = getEndpointRefsNotIncludeLeader(clusterService)
    val refNumber = allEndpointRefs.size
    // send command to all data nodes
    var countReplyRef = 0
    allEndpointRefs.par.foreach(endpointRef => {
      val res = dataNodeDriver.deleteNodeRelationship(id, relationship, direction, endpointRef, Duration.Inf)
      if (res == PandaReplyMessage.SUCCESS) {
        countReplyRef += 1
      }
      clientRpcEnv.stop(endpointRef)
    })
    clientRpcEnv.shutdown()
    if (countReplyRef == refNumber) {
      PandaReplyMessage.LEAD_NODE_SUCCESS
    } else {
      PandaReplyMessage.LEAD_NODE_FAILED
    }
  }

  override def getNodeRelationships(id: Long, address: String, port: Int, clusterService: ClusterService): ArrayBuffer[Relationship] = {
    val clientRpcEnv = HippoRpcEnvFactory.create(clientConfig)
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(address, port), config.getDataNodeEndpointName())
    val res = dataNodeDriver.getNodeRelationships(id, ref, Duration.Inf)
    clientRpcEnv.shutdown()
    res
  }

  override def createNodeRelationship(id1: Long, id2: Long, relationship: String, direction: Direction, clusterService: ClusterService): PandaReplyMessage.Value = {
    val (clientRpcEnv, allEndpointRefs) = getEndpointRefsNotIncludeLeader(clusterService)
    val refNumber = allEndpointRefs.size
    // send command to all data nodes
    var countReplyRef = 0
    allEndpointRefs.par.foreach(endpointRef => {
      val res = dataNodeDriver.createNodeRelationship(id1, id2, relationship, direction, endpointRef, Duration.Inf)
      if (res == PandaReplyMessage.SUCCESS) {
        countReplyRef += 1
      }
      clientRpcEnv.stop(endpointRef)
    })
    clientRpcEnv.shutdown()
    if (countReplyRef == refNumber) {
      PandaReplyMessage.LEAD_NODE_SUCCESS
    } else {
      PandaReplyMessage.LEAD_NODE_FAILED
    }
  }

  override def removeProperty(id: Long, property: String, clusterService: ClusterService): PandaReplyMessage.Value = {
    val (clientRpcEnv, allEndpointRefs) = getEndpointRefsNotIncludeLeader(clusterService)
    val refNumber = allEndpointRefs.size
    // send command to all data nodes
    var countReplyRef = 0
    allEndpointRefs.par.foreach(endpointRef => {
      val res = dataNodeDriver.removeProperty(id, property, endpointRef, Duration.Inf)
      if (res == PandaReplyMessage.SUCCESS) {
        countReplyRef += 1
      }
      clientRpcEnv.stop(endpointRef)
    })
    clientRpcEnv.shutdown()
    if (countReplyRef == refNumber) {
      PandaReplyMessage.LEAD_NODE_SUCCESS
    } else {
      PandaReplyMessage.LEAD_NODE_FAILED
    }
  }

  override def updateNodeLabel(id: Long, toDeleteLabel: String, newLabel: String, clusterService: ClusterService): PandaReplyMessage.Value = {
    val (clientRpcEnv, allEndpointRefs) = getEndpointRefsNotIncludeLeader(clusterService)
    val refNumber = allEndpointRefs.size
    // send command to all data nodes
    var countReplyRef = 0
    allEndpointRefs.par.foreach(endpointRef => {
      val res = dataNodeDriver.updateNodeLabel(id, toDeleteLabel, newLabel, endpointRef, Duration.Inf)
      if (res == PandaReplyMessage.SUCCESS) {
        countReplyRef += 1
      }
      clientRpcEnv.stop(endpointRef)
    })
    clientRpcEnv.shutdown()
    if (countReplyRef == refNumber) {
      PandaReplyMessage.LEAD_NODE_SUCCESS
    } else {
      PandaReplyMessage.LEAD_NODE_FAILED
    }
  }

  override def updateNodeProperty(id: Long, propertiesMap: Map[String, Any], clusterService: ClusterService): PandaReplyMessage.Value = {
    val (clientRpcEnv, allEndpointRefs) = getEndpointRefsNotIncludeLeader(clusterService)
    val refNumber = allEndpointRefs.size
    // send command to all data nodes
    var countReplyRef = 0
    allEndpointRefs.par.foreach(endpointRef => {
      val res = dataNodeDriver.updateNodeProperty(id, propertiesMap, endpointRef, Duration.Inf)
      if (res == PandaReplyMessage.SUCCESS) {
        countReplyRef += 1
      }
      clientRpcEnv.stop(endpointRef)
    })
    clientRpcEnv.shutdown()
    if (countReplyRef == refNumber) {
      PandaReplyMessage.LEAD_NODE_SUCCESS
    } else {
      PandaReplyMessage.LEAD_NODE_FAILED
    }
  }

  override def getNodesByLabel(label: String, address: String, port: Int, clusterService: ClusterService): ArrayBuffer[Node] = {
    val clientRpcEnv = HippoRpcEnvFactory.create(clientConfig)
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(address, port), config.getDataNodeEndpointName())
    val res = dataNodeDriver.getNodesByLabel(label, ref, Duration.Inf)
    clientRpcEnv.shutdown()
    res
  }

  override def getNodesByProperty(label: String, address: String, port: Int, propertiesMap: Map[String, Object], clusterService: ClusterService): ArrayBuffer[Node] = {
    val clientRpcEnv = HippoRpcEnvFactory.create(clientConfig)
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(address, port), config.getDataNodeEndpointName())
    val res = dataNodeDriver.getNodesByProperty(label, propertiesMap, ref, Duration.Inf)
    clientRpcEnv.shutdown()
    res
  }

  override def sayHello(clusterService: ClusterService): PandaReplyMessage.Value = {
    // begin cluster transaction
    //TODO: begin leader node's transaction
    val (clientRpcEnv, allEndpointRefs) = getEndpointRefsNotIncludeLeader(clusterService)
    val refNumber = allEndpointRefs.size
    // send command to all data nodes
    var countReplyRef = 0
    allEndpointRefs.par.foreach(endpointRef => {
      val res = dataNodeDriver.sayHello("hello", endpointRef, Duration.Inf)
      if (res == PandaReplyMessage.SUCCESS) {
        countReplyRef += 1
      }
    })
    println(refNumber, countReplyRef)
    clientRpcEnv.shutdown()
    if (countReplyRef == refNumber) {
      PandaReplyMessage.LEAD_NODE_SUCCESS
    } else {
      PandaReplyMessage.LEAD_NODE_FAILED
    }

    //TODO: close leader node's transaction

  }

  override def runCypher(cypher: String, address: String, port: Int, clusterService: ClusterService): InternalRecords = {
    val clientRpcEnv = HippoRpcEnvFactory.create(clientConfig)
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(address, port), config.getDataNodeEndpointName())
    val res = dataNodeDriver.runCypher(cypher, ref, Duration.Inf)
    clientRpcEnv.shutdown()
    res
  }

  override def createNode(id: Long, labels: Array[String], properties: Map[String, Any], clusterService: ClusterService): PandaReplyMessage.Value = {
    val (clientRpcEnv, allEndpointRefs) = getEndpointRefsNotIncludeLeader(clusterService)
    val refNumber = allEndpointRefs.size
    // send command to all data nodes
    var countReplyRef = 0
    allEndpointRefs.par.foreach(endpointRef => {
      val res = dataNodeDriver.createNode(id, labels, properties, endpointRef, Duration.Inf)
      if (res.isInstanceOf[Node]) {
        countReplyRef += 1
      }
      clientRpcEnv.stop(endpointRef)
    })
    clientRpcEnv.shutdown()
    if (countReplyRef == refNumber) {
      PandaReplyMessage.LEAD_NODE_SUCCESS
    } else {
      PandaReplyMessage.LEAD_NODE_FAILED
    }
  }

  override def deleteNode(id: Long, clusterService: ClusterService): PandaReplyMessage.Value = {
    val (clientRpcEnv, allEndpointRefs) = getEndpointRefsNotIncludeLeader(clusterService)
    val refNumber = allEndpointRefs.size
    // send command to all data nodes
    var countReplyRef = 0
    allEndpointRefs.par.foreach(endpointRef => {
      val res = dataNodeDriver.deleteNode(id, endpointRef, Duration.Inf)
      if (res == PandaReplyMessage.SUCCESS) {
        countReplyRef += 1
      }
      clientRpcEnv.stop(endpointRef)
    })
    clientRpcEnv.shutdown()
    if (countReplyRef == refNumber) {
      PandaReplyMessage.LEAD_NODE_SUCCESS
    } else {
      PandaReplyMessage.LEAD_NODE_FAILED
    }
  }

  override def addNodeLabel(id: Long, label: String, clusterService: ClusterService): PandaReplyMessage.Value = {
    val (clientRpcEnv, allEndpointRefs) = getEndpointRefsNotIncludeLeader(clusterService)
    val refNumber = allEndpointRefs.size
    // send command to all data nodes
    var countReplyRef = 0
    allEndpointRefs.par.foreach(endpointRef => {
      val res = dataNodeDriver.addNodeLabel(id, label, endpointRef, Duration.Inf)
      if (res == PandaReplyMessage.SUCCESS) {
        countReplyRef += 1
      }
      clientRpcEnv.stop(endpointRef)
    })
    clientRpcEnv.shutdown()
    if (countReplyRef == refNumber) {
      PandaReplyMessage.LEAD_NODE_SUCCESS
    } else {
      PandaReplyMessage.LEAD_NODE_FAILED
    }
  }


  override def getNodeById(id: Long, address: String, port: Int, clusterService: ClusterService): Node = {
    val clientRpcEnv = HippoRpcEnvFactory.create(clientConfig)
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(address, port), config.getDataNodeEndpointName())
    val res = dataNodeDriver.getNodeById(id, ref, Duration.Inf)
    clientRpcEnv.shutdown()
    res
  }


  def getEndpointRefsNotIncludeLeader(clusterService: ClusterService): (HippoRpcEnv, ArrayBuffer[HippoEndpointRef]) = {
    val dataNodes = clusterService.getDataNodes()
    val clientRpcEnv = HippoRpcEnvFactory.create(clientConfig)
    val allEndpointRefs = ArrayBuffer[HippoEndpointRef]()
    val localAddress = clusterService.getLeaderNode()
    dataNodes.map(s => {
      if (!s.equals(localAddress)) {
        val strs = s.split(":")
        val address = strs(0)
        val port = strs(1).toInt
        val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(address, port), config.getDataNodeEndpointName())
        allEndpointRefs += ref
      }
    })
    (clientRpcEnv, allEndpointRefs)
  }

  def test(driverFunc: DataNodeDriver => Any): Unit = {

  }
}
