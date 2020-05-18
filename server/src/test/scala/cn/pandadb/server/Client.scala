package cn.pandadb.server

import java.util.Random

import cn.pandadb.cluster.ClusterService
import cn.pandadb.configuration.Config
import cn.pandadb.datanode.DataNodeDriver
import cn.pandadb.leadernode.{LeaderNodeDriver, LeaderSayHello}
import cn.pandadb.util.PandaReplyMessage
import cn.pandadb.zk.ZKTools
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnvClientConfig}
import net.neoremind.kraps.rpc.netty.HippoRpcEnvFactory
import org.junit.{After, Test}
import org.neo4j.csv.reader.Extractors.DurationExtractor
import org.neo4j.graphdb.Direction

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class Client {
  val leaderDriver = new LeaderNodeDriver
  val dataNodeDriver = new DataNodeDriver
  val config = new Config
  val zkTools = new ZKTools(config)
  zkTools.init()
  val clusterService = new ClusterService(config, zkTools)
  clusterService.init()
  val clientConfig = RpcEnvClientConfig(new RpcConf(), config.getRpcServerName())
  val clientRpcEnv = HippoRpcEnvFactory.create(clientConfig)
  val str = clusterService.getLeaderNode().split(":")
  val addr = str(0)
  val port = str(1).toInt

  @Test
  def getZkDataNodes(): Unit = {
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res = leaderDriver.getZkDataNodes(ref, Duration.Inf)
    println(res)
    clientRpcEnv.stop(ref)
  }

  @Test
  def sayHello(): Unit = {
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res = leaderDriver.sayHello("hello", ref, Duration.Inf)
    println(res)
    clientRpcEnv.stop(ref)
  }

  @Test
  def runCypher(): Unit = {
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res = leaderDriver.runCypher("match (n) return n", ref, Duration.Inf)
    println(res)
    clientRpcEnv.stop(ref)
  }


  @Test
  def createNode(): Unit = {
    // ID not consistent
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val toCreate = Array(Map("aaa" -> 111), Map("bbb" -> 222), Map("ccc" -> 333), Map("ddd" -> 444), Map("eee" -> 555), Map("fff" -> 666))
    for (i <- 0 to 5) {
      val res = leaderDriver.createNode(Array("514"), toCreate(i), ref, Duration.Inf)
      println(res)
    }
    clientRpcEnv.stop(ref)
  }

  @Test
  def deleteNode(): Unit = {
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res = leaderDriver.deleteNode(22L, ref, Duration.Inf)
    println(res)
    clientRpcEnv.stop(ref)
  }

  @Test
  def addNodeLabel(): Unit = {
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res = leaderDriver.addNodeLabel(0L, "People", ref, Duration.Inf)
    println(res)
    clientRpcEnv.stop(ref)
  }

  @Test
  def getNodeById(): Unit = {
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res = leaderDriver.getNodeById(0L, ref, Duration.Inf)
    println(res)
    clientRpcEnv.stop(ref)
  }

  @Test
  def getNodesByProperty(): Unit = {
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res = leaderDriver.getNodesByProperty("514", Map("aaa" -> 111.asInstanceOf[Object]), ref, Duration.Inf)
    println(res)
    clientRpcEnv.stop(ref)
  }

  @Test
  def getNodesByLabel(): Unit = {
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res = leaderDriver.getNodesByLabel("514", ref, Duration.Inf)
    println(res)
    clientRpcEnv.stop(ref)
  }

  @Test
  def updateNodeProperty(): Unit = {
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res = leaderDriver.updateNodeProperty(0L, Map("a" -> 2, "b" -> 1), ref, Duration.Inf)
    println(res)
    clientRpcEnv.stop(ref)
  }

  @Test
  def updateNodeLabel(): Unit = {
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res = leaderDriver.updateNodeLabel(0L, "People", "boy", ref, Duration.Inf)
    println(res)
    clientRpcEnv.stop(ref)
  }

  @Test
  def removeProperty(): Unit = {
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res = leaderDriver.removeProperty(0L, "b", ref, Duration.Inf)
    println(res)
    clientRpcEnv.stop(ref)
  }

  @Test
  def createRelationship(): Unit = {
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res = leaderDriver.createNodeRelationship(0L, 1L, "friend", Direction.BOTH, ref, Duration.Inf)
    println(res)
    clientRpcEnv.stop(ref)
  }

  @Test
  def getNodeRelationships(): Unit = {
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res = leaderDriver.getNodeRelationships(0L, ref, Duration.Inf)
    println(res)
    clientRpcEnv.stop(ref)
  }

  @Test
  def deleteNodeRelationship(): Unit = {
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res = leaderDriver.deleteNodeRelationship(0L, "friend", Direction.BOTH, ref, Duration.Inf)
    println(res)
    clientRpcEnv.stop(ref)
  }

  @Test
  def getAllDBNodes(): Unit = {
    val leaderRef = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val nodes = leaderDriver.getZkDataNodes(leaderRef, Duration.Inf)
    val choose = nodes(new Random().nextInt(nodes.size))
    val strs = choose.split(":")
    val address2 = strs(0)
    val port2 = strs(1).toInt
    val dataNodeRef = clientRpcEnv.setupEndpointRef(new RpcAddress(address2, port2), config.getDataNodeEndpointName())
    val res = dataNodeDriver.getAllDBNodes(2, dataNodeRef, Duration.Inf).iterator
    while (res.hasNext) {
      println(res.next())
    }
    clientRpcEnv.stop(dataNodeRef)
    clientRpcEnv.stop(leaderRef)
  }

  @Test
  def getAllDBRelations(): Unit = {
    val leaderRef = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val nodes = leaderDriver.getZkDataNodes(leaderRef, Duration.Inf)
    val choose = nodes(new Random().nextInt(nodes.size))
    val strs = choose.split(":")
    val address2 = strs(0)
    val port2 = strs(1).toInt
    val dataNodeRef = clientRpcEnv.setupEndpointRef(new RpcAddress(address2, port2), config.getDataNodeEndpointName())
    val res = dataNodeDriver.getAllDBRelationships(2, dataNodeRef, Duration.Inf).iterator
    while (res.hasNext) {
      println(res.next())
    }
    clientRpcEnv.stop(dataNodeRef)
    clientRpcEnv.stop(leaderRef)
  }

  @After
  def close(): Unit = {
    clientRpcEnv.shutdown()
  }
}
