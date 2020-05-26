package cn.pandadb.server

import cn.pandadb.cluster.ClusterService
import cn.pandadb.configuration.Config
import cn.pandadb.datanode.DataNodeDriver
import cn.pandadb.leadernode.{LeaderNodeDriver}
import cn.pandadb.zk.ZKTools
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnvClientConfig}
import net.neoremind.kraps.rpc.netty.HippoRpcEnvFactory
import org.junit.{After, Test}
import org.neo4j.graphdb.{Direction}

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
    // return ArrayBuffer of nodes
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res = leaderDriver.runCypher("match (n) return n", ref, Duration.Inf)
    res.records.foreach(println)
    clientRpcEnv.stop(ref)
  }


  @Test
  def createNode(): Unit = {
    // return node or PandaReplyMessage.Failed
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
    // return PandaReplyMessage.Value
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res = leaderDriver.deleteNode(22L, ref, Duration.Inf)
    println(res)
    clientRpcEnv.stop(ref)
  }

  @Test
  def addNodeLabel(): Unit = {
    // return PandaReplyMessage.Value
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res = leaderDriver.addNodeLabel(0L, "People", ref, Duration.Inf)
    println(res)
    clientRpcEnv.stop(ref)
  }

  @Test
  def removeNodeLabel(): Unit = {
    // return PandaReplyMessage.Value
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res = leaderDriver.removeNodeLabel(0L, "People", ref, Duration.Inf)
    println(res)
    clientRpcEnv.stop(ref)
  }

  @Test
  def getNodeById(): Unit = {
    // return node
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res = leaderDriver.getNodeById(0L, ref, Duration.Inf)
    println(res)
    clientRpcEnv.stop(ref)
  }

  @Test
  def getNodesByProperty(): Unit = {
    // return ArrayBuffer of nodes
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res = leaderDriver.getNodesByProperty("514", Map("aaa" -> 111), ref, Duration.Inf)
    println(res.foreach(println))
    clientRpcEnv.stop(ref)
  }

  @Test
  def getNodesByLabel(): Unit = {
    // return ArrayBuffer of nodes
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res = leaderDriver.getNodesByLabel("514", ref, Duration.Inf)
    println(res.foreach(println))
    clientRpcEnv.stop(ref)
  }

  @Test
  def setNodeProperty(): Unit = {
    // return PandaReplyMessage.Value
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res = leaderDriver.setNodeProperty(0L, Map("a" -> 2, "b" -> 1), ref, Duration.Inf)
    println(res)
    clientRpcEnv.stop(ref)
  }

  @Test
  def removeNodeProperty(): Unit = {
    // return PandaReplyMessage.Value
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res = leaderDriver.removeNodeProperty(0L, "b", ref, Duration.Inf)
    println(res)
    clientRpcEnv.stop(ref)
  }

  @Test
  def createRelationship(): Unit = {
    // return relationship or PandaReplyMessage.Failed
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res = leaderDriver.createNodeRelationship(0L, 20L, "BBBB", Direction.BOTH, ref, Duration.Inf)
    val res1 = leaderDriver.createNodeRelationship(1L, 21L, "OOOO", Direction.OUTGOING, ref, Duration.Inf)
    val res2 = leaderDriver.createNodeRelationship(0L, 1L, "IIII", Direction.INCOMING, ref, Duration.Inf)
    //    leaderDriver.createNodeRelationship(21L, 22L, "LOVE", Direction.OUTGOING, ref, Duration.Inf)
    //    leaderDriver.createNodeRelationship(22L, 21L, "LOVE", Direction.OUTGOING, ref, Duration.Inf)
    println(res)
    println(res1)
    println(res2)
    clientRpcEnv.stop(ref)
  }

  @Test
  def getRelationshipByRelationId(): Unit = {
    // return relationship
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res = leaderDriver.getRelationshipByRelationId(0L, ref, Duration.Inf)
    println(res)
  }

  @Test
  def updateRelationshipProperty(): Unit = {
    // return PandaReplyMessage.Value
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val propertyMap: Map[String, Any] = Map("relation" -> "qqq", "relation2" -> 777)
    val res = leaderDriver.setRelationshipProperty(0L, propertyMap, ref, Duration.Inf)
    println(res)
  }

  @Test
  def deleteRelationshipProperty(): Unit = {
    // return PandaReplyMessage.Value
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val propertyArray: Array[String] = Array("relation2")
    val res = leaderDriver.deleteRelationshipProperties(0L, propertyArray, ref, Duration.Inf)
    println(res)
  }

  @Test
  def getNodeRelationships(): Unit = {
    // return ArrayBuffer of relationship
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res1 = leaderDriver.getNodeRelationships(2L, ref, Duration.Inf)
    val res2 = leaderDriver.getNodeRelationships(20L, ref, Duration.Inf)
    println(res1)
    println(res2)
    clientRpcEnv.stop(ref)
  }

  @Test
  def deleteNodeRelationship(): Unit = {
    // return PandaReplyMessage.Value
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
    // get stream of all nodes, then stop the ref
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
      val props = r.props
      for (m <- props) {
        println(m, "+++++++++++")
      }
    }
    clientRpcEnv.stop(dataNodeRef)
  }

  //  @Test
  //  def pullNodesAndRelationsFromDataNode(): Unit = {
  //    val dbFile = new File("D:\\DbTest\\output1\\db1")
  //    if (!dbFile.exists()) {
  //      dbFile.mkdirs
  //    }
  //    val newDb: GraphDatabaseService = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbFile).newGraphDatabase()
  //
  //    val tx = newDb.beginTx()
  //    val (res, dataNodeRef1) = leaderDriver.pullAllNodes(10, clientRpcEnv, clusterService, config)
  //    // add nodes
  //    while (res.hasNext) {
  //      val n = res.next()
  //      val node = newDb.createNode(n.id)
  //      n.labels.foreach(label => node.addLabel(Label.label(label.name)))
  //      n.props.foreach(s => node.setProperty(s._1, s._2.asAny()))
  //    }
  //    clientRpcEnv.stop(dataNodeRef1)
  //
  //    //add relationship
  //    val (res2, dataNodeRef2) = leaderDriver.pullAllRelations(10, clientRpcEnv, clusterService, config)
  //    while (res2.hasNext) {
  //      val r = res2.next()
  //      val sNode = newDb.getNodeById(r.startNode.id)
  //      val eNode = newDb.getNodeById(r.endNode.id)
  //      val rr = sNode.createRelationshipTo(eNode, RelationshipType.withName(r.relationshipType.name), r.id)
  //      if (r.props.nonEmpty) {
  //        r.props.foreach(m => {
  //          rr.setProperty(m._1, m._2.toString)
  //        })
  //      }
  //    }
  //    clientRpcEnv.stop(dataNodeRef2)
  //    tx.success()
  //    tx.close()
  //  }

  @Test
  def pullDbFiles(): Unit = {
    val localDbPath = "D:/DbTest/output1/db1" + "/"
    val dataNodeRef = leaderDriver.pullDbFileFromDataNode(localDbPath, clientRpcEnv, clusterService, config)
    clientRpcEnv.stop(dataNodeRef)
  }

  @After
  def close(): Unit = {
    clientRpcEnv.shutdown()
  }
}
