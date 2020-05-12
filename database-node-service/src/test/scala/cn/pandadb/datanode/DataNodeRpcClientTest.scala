package cn.pandadb.datanode

import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnvClientConfig}
import net.neoremind.kraps.rpc.netty.HippoRpcEnvFactory
import org.junit.{After, Before, Test}
import cn.pandadb.driver.values.{Node}
import cn.pandadb.util.PandaReplyMsg
import org.neo4j.graphdb.Direction

import scala.concurrent.duration.Duration

// before test, run DataNodeRpcServerTest
class DataNodeRpcClientTest {
  val dataNodeDriver = new DataNodeDriver
  val clientConfig = RpcEnvClientConfig(new RpcConf(), "panda-client")
  val clientRpcEnv = HippoRpcEnvFactory.create(clientConfig)
  val endpointRef = clientRpcEnv.setupEndpointRef(new RpcAddress("localhost", 6666), "server")

  var nodeTest: Node = _
  var nodeTest2: Node = _
  var nodeTest3: Node = _
  var nodeTest4: Node = _

  @Before
  def setup(): Unit = {
    val propertiesMap = Map("name" -> "gao", "age" -> 17)
    val label = Array("Person")

    val propertiesMap2 = Map("name" -> "guitar", "age" -> 17)
    val label2 = Array("Instrument")
    val propertiesMap3 = Map("name" -> "bass", "age" -> 17)
    val label3 = Array("Instrument")
    val propertiesMap4 = Map("name" -> "drum", "age" -> 17)
    val label4 = Array("Instrument")

    nodeTest = dataNodeDriver.createNode(label, propertiesMap, endpointRef, Duration.Inf)
    nodeTest2 = dataNodeDriver.createNode(label2, propertiesMap2, endpointRef, Duration.Inf)
    nodeTest3 = dataNodeDriver.createNode(label3, propertiesMap3, endpointRef, Duration.Inf)
    nodeTest4 = dataNodeDriver.createNode(label4, propertiesMap4, endpointRef, Duration.Inf)

    dataNodeDriver.createNodeRelationship(nodeTest2.id, nodeTest3.id, "enemy", Direction.OUTGOING, endpointRef, Duration.Inf)
    dataNodeDriver.createNodeRelationship(nodeTest2.id, nodeTest4.id, "friend", Direction.INCOMING, endpointRef, Duration.Inf)
    dataNodeDriver.createNodeRelationship(nodeTest3.id, nodeTest4.id, "true love", Direction.BOTH, endpointRef, Duration.Inf)
  }

  // chunkedStream
  @Test
  def getAllNodes(): Unit = {
    val res = dataNodeDriver.getAllDBNodes(2, endpointRef, Duration.Inf).iterator
    while (res.hasNext) {
      println(res.next())
    }
    //      assert(res.size == 4)
  }
  //
  //  @Test
  //  def getAllRelations(): Unit = {
  //
  //    val relations = dataNodeDriver.getAllDBRelationships(2, endpointRef, Duration.Inf)
  //    assert(relations.size == 3)
  //  }

  @Test
  def runCpyher(): Unit = {
    val s = "match (n) return n"
    val res = dataNodeDriver.runCypher(s, endpointRef, Duration.Inf)
    println(res)
  }

  // askWithBuffer
  @Test
  def createAndDeleteNode(): Unit = {
    val propertiesMap = Map("name" -> "toDelete", "age" -> 666)
    val label = Array("Person")
    val nodeToDelete = dataNodeDriver.createNode(label, propertiesMap, endpointRef, Duration.Inf)
    val res = dataNodeDriver.deleteNode(nodeToDelete.id, endpointRef, Duration.Inf)
    assert(res == PandaReplyMsg.SUCCESS)
  }

  @Test
  def getNodeById(): Unit = {
    val node = dataNodeDriver.getNodeById(nodeTest.id, endpointRef, Duration.Inf)
    assert(node == nodeTest)
  }

  @Test
  def getNodesByProperty(): Unit = {
    implicit def any2Object(x: Map[String, Any]): Map[String, Object] = x.asInstanceOf[Map[String, Object]]

    val propertyMap = Map("age" -> 17)
    val label = "Person"
    val res = dataNodeDriver.getNodesByProperty(label, propertyMap, endpointRef, Duration.Inf)
    assert(res.size == 1)
  }

  @Test
  def getNodesByLabel(): Unit = {
    val label = "Instrument"
    val res = dataNodeDriver.getNodesByLabel(label, endpointRef, Duration.Inf)
    assert(res.size == 3)
  }

  @Test
  def removeProperty(): Unit = {
    val propertyToDelete = "age"
    dataNodeDriver.removeProperty(nodeTest.id, propertyToDelete, endpointRef, Duration.Inf)
    val node = dataNodeDriver.getNodeById(nodeTest.id, endpointRef, Duration.Inf)
    assert(node.props.size == 1)
  }

  @Test
  def updateNodeProperty(): Unit = {
    val propertyMap = Map("age" -> 27)
    dataNodeDriver.updateNodeProperty(nodeTest.id, propertyMap, endpointRef, Duration.Inf)
    val node = dataNodeDriver.getNodeById(nodeTest.id, endpointRef, Duration.Inf)
    assert(node.props.apply("age").asInt() == 27)
  }

  @Test
  def updateNodeLabel(): Unit = {
    val oldLabel = "Person"
    val newLabel = "People"
    dataNodeDriver.updateNodeLabel(nodeTest.id, oldLabel, newLabel, endpointRef, Duration.Inf)
    val node = dataNodeDriver.getNodeById(nodeTest.id, endpointRef, Duration.Inf)
    assert(node.labels(0).name == newLabel)
  }

  @Test
  def addNodeLabel(): Unit = {
    val newLabel = "Player"
    dataNodeDriver.addNodeLabel(nodeTest.id, newLabel, endpointRef, Duration.Inf)
    val node = dataNodeDriver.getNodeById(nodeTest.id, endpointRef, Duration.Inf)
    assert(node.labels.length == 2)
  }

  @Test
  def getNodeRelationship(): Unit = {
    val relation2 = dataNodeDriver.getNodeRelationships(nodeTest2.id, endpointRef, Duration.Inf)
    assert(relation2.size == 2)
  }

  @Test
  def deleteNodeRelation(): Unit = {
    val relationToDelete = "enemy"
    dataNodeDriver.deleteNodeRelationship(nodeTest2.id, relationToDelete, Direction.OUTGOING, endpointRef, Duration.Inf)
    val relations = dataNodeDriver.getNodeRelationships(nodeTest2.id, endpointRef, Duration.Inf)
    assert(relations.size == 1)
  }

  @After
  def release(): Unit = {
    dataNodeDriver.deleteNode(nodeTest.id, endpointRef, Duration.Inf)
    dataNodeDriver.deleteNode(nodeTest2.id, endpointRef, Duration.Inf)
    dataNodeDriver.deleteNode(nodeTest3.id, endpointRef, Duration.Inf)
    dataNodeDriver.deleteNode(nodeTest4.id, endpointRef, Duration.Inf)
    clientRpcEnv.stop(endpointRef)
    clientRpcEnv.shutdown()
  }
}
