import cn.pandadb.neo4j.driver.values.Node
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnvClientConfig}
import net.neoremind.kraps.rpc.netty.HippoRpcEnvFactory
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
  before {
    val propertiesMap = Map("name" -> "gao", "age" -> 17)
    val label = "Person"
    nodeTest = Await.result(endpointRef.askWithBuffer[Node](CreateNode(label, propertiesMap)), Duration.Inf)
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
    val res = Await.result(endpointRef.askWithBuffer[String](DeleteNode(node)), Duration.Inf)
    assert(res == "delete node successfully!")
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

  "Delete property" should "props length should be 1 and receive delete property message" in {
    val propertyToDelete = "age"
    val message = Await.result(endpointRef.askWithBuffer[String](DeleteProperty(nodeTest, propertyToDelete)), Duration.Inf)
    val node = Await.result(endpointRef.askWithBuffer[Node](GetNodeById(nodeTest.id)), Duration.Inf)
    assert(node.props.size == 1 && message == "delete property successfully!")
  }

  "Update Node" should "get the new age" in {
    val propertyMap = Map("age" -> 27)
    val _node = Await.result(endpointRef.askWithBuffer[Node](UpdateNode(nodeTest, propertyMap, oldLabel = "", newLabel = "")), Duration.Inf)
    val node = Await.result(endpointRef.askWithBuffer[Node](GetNodeById(_node.id)), Duration.Inf)
    val age = node.props.apply("age").asInt()
    assert(age == 27)
  }

  after {
    val res = Await.result(endpointRef.askWithBuffer[String](DeleteNode(nodeTest)), Duration.Inf)
  }
}
