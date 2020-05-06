package cn.pandadb.datanode

import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnvClientConfig}
import net.neoremind.kraps.rpc.netty.HippoRpcEnvFactory
import org.junit.{After, Before, Test}
import cn.pandadb.driver.values.{Node, Relationship}
import cn.pandadb.util.ServerReplyMsg
import org.neo4j.graphdb.Direction

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration.Duration

// before test, run DataNodeRpcServerTest
class DataNodeRpcClientTest {

  val clientConfig = RpcEnvClientConfig(new RpcConf(), "panda-client")
  val clientRpcEnv = HippoRpcEnvFactory.create(clientConfig)
  val endpointRef = clientRpcEnv.setupEndpointRef(new RpcAddress("localhost", 12345), "server")

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

    nodeTest = Await.result(endpointRef.askWithBuffer[Node](CreateNode(label, propertiesMap)), Duration.Inf)
    nodeTest2 = Await.result(endpointRef.askWithBuffer[Node](CreateNode(label2, propertiesMap2)), Duration.Inf)
    nodeTest3 = Await.result(endpointRef.askWithBuffer[Node](CreateNode(label3, propertiesMap3)), Duration.Inf)
    nodeTest4 = Await.result(endpointRef.askWithBuffer[Node](CreateNode(label4, propertiesMap4)), Duration.Inf)

    Await.result(endpointRef.askWithBuffer[ServerReplyMsg.Value](CreateNodeRelationship(nodeTest2.id, nodeTest3.id, "enemy", Direction.OUTGOING)), Duration.Inf)
    Await.result(endpointRef.askWithBuffer[ServerReplyMsg.Value](CreateNodeRelationship(nodeTest2.id, nodeTest4.id, "friend", Direction.INCOMING)), Duration.Inf)
    Await.result(endpointRef.askWithBuffer[ServerReplyMsg.Value](CreateNodeRelationship(nodeTest3.id, nodeTest4.id, "true love", Direction.BOTH)), Duration.Inf)

  }

  // chunkedStream
  @Test
  def getAllNodes(): Unit = {
    val res = endpointRef.getChunkedStream[Node](GetAllDBNodes(2), Duration.Inf)
    assert(res.size == 4)
  }

  @Test
  def getAllRelations(): Unit = {
    val relations = endpointRef.getChunkedStream[Relationship](GetAllDBRelationships(2), Duration.Inf)
    assert(relations.size == 4)
  }

  // askWithBuffer
  @Test
  def createAndDeleteNode(): Unit = {
    val propertiesMap = Map("name" -> "toDelete", "age" -> 666)
    val label = Array("Person")
    val nodeToDelete = Await.result(endpointRef.askWithBuffer[Node](CreateNode(label, propertiesMap)), Duration.Inf)
    val res = Await.result(endpointRef.askWithBuffer[ServerReplyMsg.Value](DeleteNode(nodeToDelete.id)), Duration.Inf)
    assert(res == ServerReplyMsg.SUCCESS)
  }

  @Test
  def getNodeById(): Unit = {
    val node = Await.result(endpointRef.askWithBuffer[Node](GetNodeById(nodeTest.id)), Duration.Inf)
    assert(node == nodeTest)
  }

  @Test
  def getNodesByProperty(): Unit = {
    implicit def any2Object(x: Map[String, Any]): Map[String, Object] = x.asInstanceOf[Map[String, Object]]

    val propertyMap = Map("age" -> 17)
    val label = "Person"
    val res = Await.result(endpointRef.askWithBuffer[ArrayBuffer[Node]](GetNodesByProperty(label, propertyMap)), Duration.Inf)
    assert(res.size == 1)
  }

  @Test
  def getNodesByLabel(): Unit = {
    val label = "Instrument"
    val res = Await.result(endpointRef.askWithBuffer[ArrayBuffer[Node]](GetNodesByLabel(label)), Duration.Inf)
    assert(res.size == 3)
  }

  @Test
  def removeProperty(): Unit = {
    val propertyToDelete = "age"
    Await.result(endpointRef.askWithBuffer[ServerReplyMsg.Value](RemoveProperty(nodeTest.id, propertyToDelete)), Duration.Inf)
    val node = Await.result(endpointRef.askWithBuffer[Node](GetNodeById(nodeTest.id)), Duration.Inf)
    assert(node.props.size == 1)
  }

  @Test
  def updateNodeProperty(): Unit = {
    val propertyMap = Map("age" -> 27)
    Await.result(endpointRef.askWithBuffer[ServerReplyMsg.Value](UpdateNodeProperty(nodeTest.id, propertyMap)), Duration.Inf)
    val node = Await.result(endpointRef.askWithBuffer[Node](GetNodeById(nodeTest.id)), Duration.Inf)
    assert(node.props.apply("age").asInt() == 27)
  }

  @Test
  def updateNodeLabel(): Unit = {
    val oldLabel = "Person"
    val newLabel = "People"
    Await.result(endpointRef.askWithBuffer[ServerReplyMsg.Value](UpdateNodeLabel(nodeTest.id, oldLabel, newLabel)), Duration.Inf)
    val node = Await.result(endpointRef.askWithBuffer[Node](GetNodeById(nodeTest.id)), Duration.Inf)
    assert(node.labels(0).name == newLabel)
  }

  @Test
  def getNodeRelationship(): Unit = {
    val relation2 = Await.result(endpointRef.askWithBuffer[ArrayBuffer[Relationship]](GetNodeRelationships(nodeTest2.id)), Duration.Inf)
    assert(relation2.size == 2)
  }

  @Test
  def deleteNodeRelation(): Unit = {
    val relationToDelete = "enemy"
    Await.result(endpointRef.askWithBuffer[ServerReplyMsg.Value](DeleteNodeRelationship(nodeTest2.id, relationToDelete, Direction.OUTGOING)), Duration.Inf)
    val relations = Await.result(endpointRef.askWithBuffer[ArrayBuffer[Node]](GetNodeRelationships(nodeTest2.id)), Duration.Inf)
    assert(relations.size == 1)
  }

  @After
  def release(): Unit = {
    Await.result(endpointRef.askWithBuffer[ServerReplyMsg.Value](DeleteNode(nodeTest.id)), Duration.Inf)
    Await.result(endpointRef.askWithBuffer[ServerReplyMsg.Value](DeleteNode(nodeTest2.id)), Duration.Inf)
    Await.result(endpointRef.askWithBuffer[ServerReplyMsg.Value](DeleteNode(nodeTest3.id)), Duration.Inf)
    Await.result(endpointRef.askWithBuffer[ServerReplyMsg.Value](DeleteNode(nodeTest4.id)), Duration.Inf)
    clientRpcEnv.stop(endpointRef)
    clientRpcEnv.shutdown()
  }
}
