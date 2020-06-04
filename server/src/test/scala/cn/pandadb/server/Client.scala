package cn.pandadb.server

import cn.pandadb.cluster.ClusterService
import cn.pandadb.configuration.Config
import cn.pandadb.datanode.DataNodeDriver
import cn.pandadb.leadernode.LeaderNodeDriver
import cn.pandadb.zk.ZKTools
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnvClientConfig}
import net.neoremind.kraps.rpc.netty.HippoRpcEnvFactory
import org.junit.{After, Test}
import cn.pandadb.driver.values.Direction
import cn.pandadb.util.CompressDbFileUtil

import scala.concurrent.duration.Duration

class Client {
  val leaderDriver = new LeaderNodeDriver
  val dataNodeDriver = new DataNodeDriver
  val config = new Config
  //  val zkTools = new ZKTools(config)
  //  zkTools.init()
  val clusterService = new ClusterService(config)
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
    clientRpcEnv.stop(ref)
  }

  @Test
  def sayHello(): Unit = {
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res = leaderDriver.sayHello("hello", ref, Duration.Inf)
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
    val toCreate = Array(
      Map("aaa" -> 111), Map("bbb" -> 222),
      Map("ccc" -> 333), Map("ddd" -> 444),
      Map("eee" -> 555), Map("fff" -> 666)
    )
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
    //    println(res)
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
    val res = leaderDriver.createNodeRelationship(
      0L, 20L, "QQQQ", Direction.BOTH, ref, Duration.Inf
    )
    val res1 = leaderDriver.createNodeRelationship(
      1L, 2L, "OOOO", Direction.OUTGOING, ref, Duration.Inf
    )
    val res2 = leaderDriver.createNodeRelationship(
      2L, 1L, "IIII", Direction.INCOMING, ref, Duration.Inf
    )
    //    println(res)
    //    println(res1)
    //    println(res2)
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
  def setRelationshipProperty(): Unit = {
    // return PandaReplyMessage.Value
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val propertyMap: Map[String, Any] = Map("relation" -> "qqq", "relation2" -> 777)
    val res = leaderDriver.setRelationshipProperty(0L, propertyMap, ref, Duration.Inf)
    //    println(res)
  }

  @Test
  def deleteRelationshipProperty(): Unit = {
    // return PandaReplyMessage.Value
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val propertyArray: Array[String] = Array("relation2")
    val res = leaderDriver.deleteRelationshipProperties(0L, propertyArray, ref, Duration.Inf)
    //    println(res)
  }

  @Test
  def getNodeRelationships(): Unit = {
    // return ArrayBuffer of relationship
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res1 = leaderDriver.getNodeRelationships(2L, ref, Duration.Inf)
    println(res1.foreach(println))
    clientRpcEnv.stop(ref)
  }

  @Test
  def deleteNodeRelationship(): Unit = {
    // return PandaReplyMessage.Value
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res = leaderDriver.deleteNodeRelationship(
      0L, 20L, "QQQQ", Direction.BOTH, ref, Duration.Inf
    )
    //    println(res)
    clientRpcEnv.stop(ref)
  }

  @Test
  def getAllDBNodes(): Unit = {
    // stream
    val (res, dataNodeRef) = leaderDriver.pullAllNodes(10, clientRpcEnv, clusterService, config)
    while (res.hasNext) {
      val node = res.next()
      println(node)
    }
    clientRpcEnv.stop(dataNodeRef)
  }

  @Test
  def getAllDBRelations(): Unit = {
    // stream
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

  @Test
  def getAllDBLabels(): Unit = {
    // stream
    val (res, dataNodeRef) = leaderDriver.pullAllLabels(10, clientRpcEnv, clusterService, config)
    while (res.hasNext) {
      val r = res.next()
      println(r.name)
    }
    clientRpcEnv.stop(dataNodeRef)
  }


  @Test
  def pullDbFiles(): Unit = {
    // model 1: get file names from leader node then download file one by one from data node.
    val localDbPath = "F:/CNIC/DbTest/"
    val dataNodeRef = leaderDriver.pullDbFileFromDataNode(localDbPath, clientRpcEnv, clusterService, config)
    clientRpcEnv.stop(dataNodeRef)
  }

  @Test
  def pullCompressedDbFile(): Unit = {
    // model 2: random choose a data node, then data node compress Db files and send the compressed file.
    val downloadZipPath = "F:/CNIC/DbTest/compressTest/"
    val zipFileName = "DB_FILES.zip"
    val decompressTo = "F:/CNIC/DbTest/decompress/"
    val ref = leaderDriver.pullCompressedDbFileFromDataNode(
      downloadZipPath, zipFileName, clientRpcEnv, clusterService, config
    )
    clientRpcEnv.stop(ref)
    // deCompress
    val compressUtil = new CompressDbFileUtil
    compressUtil.decompress(downloadZipPath + zipFileName, decompressTo)
  }


  @After
  def close(): Unit = {
    clientRpcEnv.shutdown()
  }
}
