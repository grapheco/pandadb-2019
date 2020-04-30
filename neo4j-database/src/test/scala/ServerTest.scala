import cn.pandadb.neo4j.driver.values.{Node, Relationship}
import cn.pandadb.neo4j.util.ServerReplyMsg
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnvClientConfig}
import net.neoremind.kraps.rpc.netty.HippoRpcEnvFactory
import org.neo4j.graphdb.Direction
import org.scalatest.{BeforeAndAfter, FlatSpec, FunSuite}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration.Duration

// before test, should run test.scala.Server.scala

class ServerTest extends FunSuite with BeforeAndAfter {
  val configClient = RpcEnvClientConfig(new RpcConf(), "client")
  val rpcEnvClient = HippoRpcEnvFactory.create(configClient)
  val endpointRef = rpcEnvClient.setupEndpointRef(new RpcAddress("localhost", 12345), "server")

  var nodeTest: Node = _
  var nodeTest2: Node = _
  var nodeTest3: Node = _
  var nodeTest4: Node = _
  before {
    val propertiesMap = Map("name" -> "gao", "age" -> 17)
    val label = "Person"
    nodeTest = Await.result(endpointRef.askWithBuffer[Node](CreateNode(label, propertiesMap)), Duration.Inf)

    val propertiesMap2 = Map("name" -> "guitar", "age" -> 1)
    val label2 = "Person"
    val propertiesMap3 = Map("name" -> "bass", "age" -> 2)
    val label3 = "Person"
    val propertiesMap4 = Map("name" -> "drum", "age" -> 3)
    val label4 = "Person"
    nodeTest2 = Await.result(endpointRef.askWithBuffer[Node](CreateNode(label2, propertiesMap2)), Duration.Inf)
    nodeTest3 = Await.result(endpointRef.askWithBuffer[Node](CreateNode(label3, propertiesMap3)), Duration.Inf)
    nodeTest4 = Await.result(endpointRef.askWithBuffer[Node](CreateNode(label4, propertiesMap4)), Duration.Inf)

    Await.result(endpointRef.askWithBuffer[ServerReplyMsg.Value](CreateRelationshipTo(nodeTest2, nodeTest3, "enemy")), Duration.Inf)
    Await.result(endpointRef.askWithBuffer[ServerReplyMsg.Value](CreateRelationshipTo(nodeTest2, nodeTest4, "friend")), Duration.Inf)

  }

  //  test("hippo getChunkedStream"){
  //    val res  = endpointRef.getChunkedStream[Node](GetChunkedNodeStream(2), Duration.Inf)
  //    println(res.foreach(n=>n.props))
  //  }

  test("Server rpc test") {
    val res = Await.result(endpointRef.askWithBuffer[SayHelloResponse](SayHelloRequest("hello")), Duration.Inf)
    assert(res.value == "HELLO")
  }

  test("Create node and delete node test") {
    val propertiesMap = Map("name" -> "Liam", "age" -> 188888)
    val label = "Person"
    val node = Await.result(endpointRef.askWithBuffer[Node](CreateNode(label, propertiesMap)), Duration.Inf)
    val name = node.props.apply("name").asString()
    val age = node.props.apply("age").asInt()
    assert(name == "Liam" && age == 188888)
    val msg = Await.result(endpointRef.askWithBuffer[ServerReplyMsg.Value](DeleteNode(node)), Duration.Inf)
    assert(msg == ServerReplyMsg.SUCCESS)
  }

  test("Get node by id") {
    val node = Await.result(endpointRef.askWithBuffer[Node](GetNodeById(nodeTest.id)), Duration.Inf)
    val name = node.props.apply("name").asString()
    val age = node.props.apply("age").asInt()
    assert(name == "gao" && age == 17)
  }

  test("Get node by property") {
    implicit def any2Object(x: Map[String, Any]): Map[String, Object] = x.asInstanceOf[Map[String, Object]]

    val propertyMap = Map("name" -> "gao", "age" -> 17)
    val label = "Person"
    val res = Await.result(endpointRef.askWithBuffer[ArrayBuffer[Node]](GetNodesByProperty(label, propertyMap)), Duration.Inf)
    val node = res(0)
    val name = node.props.apply("name").asString()
    val age = node.props.apply("age").asInt()
    assert(name == "gao" && age == 17)
  }

  test("Delete property") {
    val propertyToDelete = "age"
    val message = Await.result(endpointRef.askWithBuffer[ServerReplyMsg.Value](DeleteProperty(nodeTest, propertyToDelete)), Duration.Inf)
    assert(message == ServerReplyMsg.SUCCESS)
    val node = Await.result(endpointRef.askWithBuffer[Node](GetNodeById(nodeTest.id)), Duration.Inf)
    assert(node.props.size == 1)
  }

  test("Update Node property") {
    val propertyMap = Map("age" -> 27)
    val result = Await.result(endpointRef.askWithBuffer[ServerReplyMsg.Value](UpdateNodeProperty(nodeTest, propertyMap)), Duration.Inf)
    assert(result == ServerReplyMsg.SUCCESS)
  }

  test("Update Node label") {
    val oldLabel = "Person"
    val newLabel = "People"
    val result = Await.result(endpointRef.askWithBuffer[ServerReplyMsg.Value](UpdateNodeLabel(nodeTest, oldLabel, newLabel)), Duration.Inf)
    assert(result == ServerReplyMsg.SUCCESS)
  }

  test("Create relationship to") {
    val propertiesMap = Map("name" -> "Liam", "age" -> 18)
    val label = "Person"
    val relation = "same person"
    val node = Await.result(endpointRef.askWithBuffer[Node](CreateNode(label, propertiesMap)), Duration.Inf)
    val result = Await.result(endpointRef.askWithBuffer[ServerReplyMsg.Value](CreateRelationshipTo(nodeTest, node, relation)), Duration.Inf)
    assert(result == ServerReplyMsg.SUCCESS)
    Await.result(endpointRef.askWithBuffer[ServerReplyMsg.Value](DeleteNode(node)), Duration.Inf)
  }

  test("Get node relationships") {
    val relations = Await.result(endpointRef.askWithBuffer[ArrayBuffer[Relationship]](GetNodeRelationships(nodeTest2)), Duration.Inf)
    assert(relations.size == 2)
  }

  test("delete node relation") {
    val relationToDelete = "enemy"
    val res = Await.result(endpointRef.askWithBuffer[ServerReplyMsg.Value](DeleteRelationship(nodeTest2, relationToDelete, Direction.OUTGOING)), Duration.Inf)
    assert(res == ServerReplyMsg.SUCCESS)
  }

  test("Get all nodes") {
    val nodes = Await.result(endpointRef.askWithBuffer[ArrayBuffer[Node]](GetAllNodes()), Duration.Inf)
    assert(nodes.size == 4)
  }

  test("Get all relationships") {
    val relationships = Await.result(endpointRef.askWithBuffer[ArrayBuffer[Relationship]](GetAllRelationships()), Duration.Inf)
    assert(relationships.size == 2)
  }

  after {
    println("enter finish")
    val a = Await.result(endpointRef.askWithBuffer[ServerReplyMsg.Value](DeleteNode(nodeTest)), Duration.Inf)
    println(a)
    Await.result(endpointRef.askWithBuffer[ServerReplyMsg.Value](DeleteNode(nodeTest2)), Duration.Inf)
    Await.result(endpointRef.askWithBuffer[ServerReplyMsg.Value](DeleteNode(nodeTest3)), Duration.Inf)
    Await.result(endpointRef.askWithBuffer[ServerReplyMsg.Value](DeleteNode(nodeTest4)), Duration.Inf)
    rpcEnvClient.shutdown()
  }
}
