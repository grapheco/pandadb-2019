package cn.pandadb.server

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
  }

  @Test
  def sayHello(): Unit = {
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res = leaderDriver.sayHello("hello", ref, Duration.Inf)
    println(res)
  }

  @Test
  def runCypher(): Unit = {
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res = leaderDriver.runCypher("match (n) return n", ref, Duration.Inf)
    println(res)
  }

  @Test
  def runCypherOnAllNodes(): Unit = {
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res = leaderDriver.runCypherOnAllNodes("match (n) return n", ref, Duration.Inf)
    println(res(0))
    println(res(1))
  }

  @Test
  def createNode(): Unit = {
    // one ref can only use less than 4 times, otherwise it will stuck...
    val toCreate = Array(Map("aaa" -> 111), Map("bbb" -> 222), Map("ccc" -> 333), Map("ddd" -> 444), Map("eee" -> 555), Map("fff" -> 666))
    for (i <- 0 to 5) {
      val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
      val res = leaderDriver.createNode(Array("514"), toCreate(i), ref, Duration.Inf)
      clientRpcEnv.stop(ref)
      println(res)
    }
  }

  @Test
  def bugTest(): Unit = {
    val ref1 = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res1 = Await.result(ref1.askWithBuffer[PandaReplyMessage.Value](LeaderSayHello("hello")), Duration.Inf)
    val res2 = Await.result(ref1.askWithBuffer[PandaReplyMessage.Value](LeaderSayHello("hello")), Duration.Inf)
    val res3 = Await.result(ref1.askWithBuffer[PandaReplyMessage.Value](LeaderSayHello("hello")), Duration.Inf)
    val res4 = Await.result(ref1.askWithBuffer[PandaReplyMessage.Value](LeaderSayHello("hello")), Duration.Inf)
    val res5 = Await.result(ref1.askWithBuffer[PandaReplyMessage.Value](LeaderSayHello("hello")), Duration.Inf)
    val res6 = Await.result(ref1.askWithBuffer[PandaReplyMessage.Value](LeaderSayHello("hello")), Duration.Inf)

    println(res1)
    println(res2)
    println(res3)
    println(res4)
    println(res5)
    println(res6)

  }


  @Test
  def deleteNode(): Unit = {
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res = leaderDriver.deleteNode(0L, ref, Duration.Inf)
    println(res)
  }

  @Test
  def addNodeLabel(): Unit = {
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res = leaderDriver.addNodeLabel(0L, "People", ref, Duration.Inf)
    println(res)
  }

  @Test
  def getNodeById(): Unit = {
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res = leaderDriver.getNodeById(0L, ref, Duration.Inf)
    println(res)
  }

  @Test
  def getNodesByProperty(): Unit = {
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res = leaderDriver.getNodesByProperty("Person", Map("aaa" -> 111.asInstanceOf[Object]), ref, Duration.Inf)
    println(res)
  }

  @Test
  def getNodesByLabel(): Unit = {
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res = leaderDriver.getNodesByLabel("Person", ref, Duration.Inf)
    println(res)
  }

  @Test
  def updateNodeProperty(): Unit = {
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res = leaderDriver.updateNodeProperty(0L, Map("a" -> 2, "b" -> 1), ref, Duration.Inf)
    println(res)
  }

  @Test
  def updateNodeLabel(): Unit = {
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res = leaderDriver.updateNodeLabel(0L, "People", "boy", ref, Duration.Inf)
    println(res)
  }

  @Test
  def removeProperty(): Unit = {
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res = leaderDriver.removeProperty(0L, "b", ref, Duration.Inf)
    println(res)
  }

  @Test
  def createRelationship(): Unit = {
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res = leaderDriver.createNodeRelationship(0L, 1L, "friend", Direction.BOTH, ref, Duration.Inf)
    println(res)
  }

  @Test
  def getNodeRelationships(): Unit = {
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res = leaderDriver.getNodeRelationships(0L, ref, Duration.Inf)
    println(res)
  }

  @Test
  def deleteNodeRelationship(): Unit = {
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res = leaderDriver.deleteNodeRelationship(0L, "friend", Direction.BOTH, ref, Duration.Inf)
    println(res)
  }

  //  @Test
  //  def getAllDBNodes(): Unit ={
  //    //get data from local database
  //    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
  //    val res = leaderDriver.getAllDBNodes(2, ref, Duration.Inf).iterator
  //    while (res.hasNext){
  //      println(res.next())
  //    }
  //  }

  @After
  def close(): Unit = {
    clientRpcEnv.shutdown()
  }

  //    val res = leaderDriver.sayHello("hello", ref, Duration.Inf)
  //    val res = leaderDriver.runCypher("match (n) return n", ref, Duration.Inf)
  //    val res = leaderDriver.createNode(Array("Person"), Map("a"->1), ref, Duration.Inf)

}
