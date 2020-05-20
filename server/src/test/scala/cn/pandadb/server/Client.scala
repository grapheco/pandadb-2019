package cn.pandadb.server

import java.io.File
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
import org.neo4j.graphdb.{Direction, GraphDatabaseService, Label, RelationshipType}
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.values.storable.Value

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
    leaderDriver.createNodeRelationship(2L, 20L, "enemy", Direction.BOTH, ref, Duration.Inf)
    //    leaderDriver.createNodeRelationship(1L, 0L, "friend", Direction.OUTGOING, ref, Duration.Inf)
    //    leaderDriver.createNodeRelationship(0L, 1L, "friend", Direction.OUTGOING, ref, Duration.Inf)
    //    leaderDriver.createNodeRelationship(21L, 22L, "LOVE", Direction.OUTGOING, ref, Duration.Inf)
    //    leaderDriver.createNodeRelationship(22L, 21L, "LOVE", Direction.OUTGOING, ref, Duration.Inf)

    clientRpcEnv.stop(ref)
  }

  @Test
  def getNodeRelationships(): Unit = {
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res1 = leaderDriver.getNodeRelationships(2L, ref, Duration.Inf)
    val res2 = leaderDriver.getNodeRelationships(20L, ref, Duration.Inf)
    println(res1)
    println(res2)
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
    val (res, dataNodeRef) = leaderDriver.pullAllNodes(10, clientRpcEnv, clusterService, config)
    while (res.hasNext) {
      val node = res.next()
      println(node)
      val props = node.props
      for (m <- props) {
        println(m._2.asAny(), "----")
      }
    }
    clientRpcEnv.stop(dataNodeRef)
  }

  @Test
  def getAllDBRelations(): Unit = {
    val (res, dataNodeRef) = leaderDriver.pullAllRelations(10, clientRpcEnv, clusterService, config)
    while (res.hasNext) {
      val r = res.next()
      println(r.id)
      println(r.startNode.id, r.endNode.id)
      println(r.relationshipType, r.relationshipType.name)
      println("-===============-")
      println(r.props.isEmpty)
    }
    clientRpcEnv.stop(dataNodeRef)
  }

  @Test
  def pullNodesAndRelationsFromDataNode(): Unit = {
    val dbFile = new File("D:\\DbTest\\output1\\db1")
    if (!dbFile.exists()) {
      dbFile.mkdirs
    }
    val newDb: GraphDatabaseService = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbFile).newGraphDatabase()

    val tx = newDb.beginTx()
    val (res, dataNodeRef1) = leaderDriver.pullAllNodes(10, clientRpcEnv, clusterService, config)
    // add nodes
    while (res.hasNext) {
      val n = res.next()
      val node = newDb.createNode(n.id)
      n.labels.foreach(label => node.addLabel(Label.label(label.name)))
      n.props.foreach(s => node.setProperty(s._1, s._2.asAny()))
      println("success add node id: ", n.id)
    }
    clientRpcEnv.stop(dataNodeRef1)

    //add relationship
    val (res2, dataNodeRef2) = leaderDriver.pullAllRelations(10, clientRpcEnv, clusterService, config)
    while (res2.hasNext) {
      val r = res2.next()
      val sNode = newDb.getNodeById(r.startNode.id)
      val eNode = newDb.getNodeById(r.endNode.id)
      val rr = sNode.createRelationshipTo(eNode, RelationshipType.withName(r.relationshipType.name), r.id)
      if (r.props.nonEmpty) {
        r.props.foreach(m => rr.setProperty(m._1, m._2))
      }
    }
    clientRpcEnv.stop(dataNodeRef2)
    tx.success()
    tx.close()
  }

  @After
  def close(): Unit = {
    clientRpcEnv.shutdown()
  }
}
