import cn.pandadb.neo4j.driver.values.{Node, Relationship}
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnvClientConfig}
import net.neoremind.kraps.rpc.netty.HippoRpcEnvFactory
import org.neo4j.graphdb.Direction
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration.Duration

// before test, should run test.scala.Server.scala

class ServerTest extends FlatSpec with BeforeAndAfter {
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

    Await.result(endpointRef.askWithBuffer[Relationship](CreateRelationshipTo(nodeTest2, nodeTest3, "enemy")), Duration.Inf)
    Await.result(endpointRef.askWithBuffer[Relationship](CreateRelationshipTo(nodeTest2, nodeTest4, "friend")), Duration.Inf)

  }

  "Server rpc test" should "receive HELLO" in {
    val res = Await.result(endpointRef.askWithBuffer[SayHelloResponse](SayHelloRequest("hello")), Duration.Inf)
    assert(res.value == "HELLO")
  }

  "Create node and delete node test" should "get Liam and 18 and delete message" in {
    val propertiesMap = Map("name" -> "Liam", "age" -> 18)
    val label = "Person"
    val node = Await.result(endpointRef.askWithBuffer[Node](CreateNode(label, propertiesMap)), Duration.Inf)
    val name = node.props.apply("name").asString()
    val age = node.props.apply("age").asInt()
    assert(name == "Liam" && age == 18)
    val msg = Await.result(endpointRef.askWithBuffer[String](DeleteNode(node)), Duration.Inf)
    assert(msg == "delete node successfully!")
  }

  "Get node by id" should "let node equals to nodeTest" in {
    val node = Await.result(endpointRef.askWithBuffer[Node](GetNodeById(nodeTest.id)), Duration.Inf)
    val name = node.props.apply("name").asString()
    val age = node.props.apply("age").asInt()
    assert(name == "gao" && age == 17)
  }

  "Get node by property" should "let node equals to nodeTest" in {
    implicit def any2Object(x: Map[String, Any]): Map[String, Object] = x.asInstanceOf[Map[String, Object]]

    val propertyMap = Map("name" -> "gao", "age" -> 17)
    val label = "Person"
    val res = Await.result(endpointRef.askWithBuffer[ArrayBuffer[Node]](GetNodesByProperty(label, propertyMap)), Duration.Inf)
    val node = res(0)
    val name = node.props.apply("name").asString()
    val age = node.props.apply("age").asInt()
    assert(name == "gao" && age == 17)
  }

  "Delete property" should "let props length equals to 1 and receive delete property message" in {
    val propertyToDelete = "age"
    val message = Await.result(endpointRef.askWithBuffer[String](DeleteProperty(nodeTest, propertyToDelete)), Duration.Inf)
    assert(message == "delete property successfully!")
    val node = Await.result(endpointRef.askWithBuffer[Node](GetNodeById(nodeTest.id)), Duration.Inf)
    assert(node.props.size == 1)
  }

  "Update Node" should "get the new age and label" in {
    val propertyMap = Map("age" -> 27)
    val oldLabel = "Person"
    val newLabel = "People"
    //if you don't want to change the label, just use UpdateNode(nodeTest, propertyMap)
    val node = Await.result(endpointRef.askWithBuffer[Node](UpdateNode(nodeTest, propertyMap, oldLabel = oldLabel, newLabel = newLabel)), Duration.Inf)
    val age = node.props.apply("age").asInt()
    assert(age == 27 && node.labels.apply(0).name == "People")
  }

  //TODO: three type of direction should be test
  "Create relationship" should "get the relationship" in {
    val propertiesMap = Map("name" -> "Liam", "age" -> 18)
    val label = "Person"
    val relation = "same person"
    val node = Await.result(endpointRef.askWithBuffer[Node](CreateNode(label, propertiesMap)), Duration.Inf)
    val relationship = Await.result(endpointRef.askWithBuffer[Relationship](CreateRelationshipTo(nodeTest, node, relation)), Duration.Inf)
    assert(relationship.relationshipType.name == relation)
    Await.result(endpointRef.askWithBuffer[String](DeleteNode(node)), Duration.Inf)
  }

  "Get node relationships" should "get node's all relations" in {
    val relations = Await.result(endpointRef.askWithBuffer[ArrayBuffer[Relationship]](GetNodeRelationships(nodeTest2)), Duration.Inf)
    assert(relations.size == 2)
  }

  //TODO: three type of direction should be test
  "delete node relation" should "pass the code" in {
    val relationToDelete = "enemy"
    val res = Await.result(endpointRef.askWithBuffer[String](DeleteRelationship(nodeTest2, relationToDelete, Direction.OUTGOING)), Duration.Inf)
    assert(res == "delete relationship successfully!")
  }

  "Get all nodes" should "get db's all nodes" in {
    val nodes = Await.result(endpointRef.askWithBuffer[ArrayBuffer[Node]](GetAllNodes()), Duration.Inf)
    assert(nodes.size == 4)
  }

  "Get all relationships" should "get db's all relations" in {
    val relationships = Await.result(endpointRef.askWithBuffer[ArrayBuffer[Relationship]](GetAllRelationships()), Duration.Inf)
    assert(relationships.size == 2)
  }

  after {
    Await.result(endpointRef.askWithBuffer[String](DeleteNode(nodeTest)), Duration.Inf)
    Await.result(endpointRef.askWithBuffer[String](DeleteNode(nodeTest2)), Duration.Inf)
    Await.result(endpointRef.askWithBuffer[String](DeleteNode(nodeTest3)), Duration.Inf)
    Await.result(endpointRef.askWithBuffer[String](DeleteNode(nodeTest4)), Duration.Inf)
    rpcEnvClient.shutdown()
  }
}
