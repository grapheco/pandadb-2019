import java.io.File
import java.nio.ByteBuffer

import cn.pandadb.neo4j.driver.values.{Node, Relationship}
import cn.pandadb.neo4j.util.ValueConverter
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.{RpcCallContext, RpcEndpoint, RpcEnvServerConfig}
import net.neoremind.kraps.rpc.netty.{HippoRpcEnv, HippoRpcEnvFactory}
import org.grapheco.hippo.{HippoRpcHandler, ReceiveContext}
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.graphdb.{Direction, GraphDatabaseService, Label, RelationshipType}

import scala.collection.JavaConversions
import scala.collection.mutable.ArrayBuffer

object Server {
  def main(args: Array[String]): Unit = {
    val config = RpcEnvServerConfig(new RpcConf(), "server", "localhost", 12345)
    val rpcEnv = HippoRpcEnvFactory.create(config)
    val endpoint = new MyEndpoint(rpcEnv)
    rpcEnv.setupEndpoint("server", endpoint)
    rpcEnv.setRpcHandler(endpoint)
    rpcEnv.awaitTermination()
  }
}

class MyEndpoint(override val rpcEnv: HippoRpcEnv) extends RpcEndpoint with HippoRpcHandler {
  val db: GraphDatabaseService = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(
    new File("./output/testdb")).newGraphDatabase()

  override def onStart(): Unit = {
    println("start panda server rpc endpoint")
    //            val tx = db.beginTx()
    //            val query =
    //              """
    //                |CREATE (n1:Person { name:'test01', age:10, adult:False, idcard:230715199809070019})
    //                |CREATE (n2:Person:Man { name:'test02', age:20, salary:30000.987, adult:True})
    //                |CREATE (n3:Person { name:'test03',born1:date('2019-01-01'), born2:time('12:05:01'), born3:datetime('2019-01-02T12:05:15[Australia/Eucla]'),
    //                |born4:datetime('2015-06-24T12:50:35.000000556Z'), born5:localtime(), born6:localdatetime(), dur:duration({days:1})} )
    //                |CREATE p =(vic:Person{ name:'vic',title:"Developer" })-[:WorksAt]->(michael:Person { name: 'Michael',title:"Manager" })
    //                |CREATE (n4:Person:Student{name: 'test04',age:15, sex:'male', school: 'No1 Middle School'}),
    //                |(n5:Person:Teacher{name: 'test05', age: 30, sex:'male', school: 'No1 Middle School', class: 'math'}),
    //                |(n6:Person:Teacher{name: 'test06', age: 40, sex:'female', school: 'No1 Middle School', class: 'chemistry'})
    //                |CREATE (n7:Person { name:'test07', age:10})-[r:WorksAt{name:'test08', age:10, adult:False, born:'2020/03/05'}]->(neo:Company{business:'Software'})
    //                |create (n8:Person{name:'point2d',location:point({longitude: 12.78, latitude: 56.7 })})
    //                |create (n9:Person{name:'point3d', location:point({longitude: 12.78, latitude: 56.7, height: 100 })})
    //                |create (n10:Person { name:'test08',titles:['ceo','ui','dev'], salaries: [10000, 20000, 30597, 500954], boolattr: [False, True, false, true]})
    //              """.stripMargin
    //            db.execute(query)
    //            tx.success()
    //            tx.close()
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case SayHelloRequest(msg) => {
      context.reply(SayHelloResponse(s"$msg response"))
    }
  }

  override def receiveWithBuffer(extraInput: ByteBuffer, context: ReceiveContext): PartialFunction[Any, Unit] = {
    case SayHelloRequest(msg) => {
      context.reply(SayHelloResponse(msg.toUpperCase()))
    }
    case GetNodeById(id) => {
      val tx = db.beginTx()
      val res = db.getNodeById(id)
      val node = ValueConverter.toDriverNode(res)
      tx.success()
      tx.close()
      context.reply(node)
    }
    case GetNodesByProperty(label, kvMap) => {
      val lst = ArrayBuffer[Node]()
      val tx = db.beginTx()
      val _label = Label.label(label)
      val propertyMaps = JavaConversions.mapAsJavaMap(kvMap)
      val res = db.findNodes(_label, propertyMaps)
      while (res.hasNext) {
        val node = res.next()
        val driverNode = ValueConverter.toDriverNode(node)
        lst += driverNode
      }
      tx.success()
      tx.close()
      context.reply(lst)
    }

    case CreateNode(label, propertiesMaps) => {
      val tx = db.beginTx()
      val node = db.createNode()
      node.addLabel(Label.label(label))
      for (map <- propertiesMaps) {
        node.setProperty(map._1, map._2)
      }
      val driverNode = ValueConverter.toDriverNode(node)
      tx.success()
      tx.close()
      context.reply(driverNode)
    }
    case DeleteNode(node) => {
      val tx = db.beginTx()
      val id = node.id
      val db_node = db.getNodeById(id)
      if (db_node.hasRelationship) {
        val relationships = db_node.getRelationships().iterator()
        while (relationships.hasNext) {
          relationships.next().delete()
        }
      }
      db_node.delete()
      tx.success()
      tx.close()
      context.reply("delete node successfully!")
    }

    case DeleteProperty(node, property) => {
      val tx = db.beginTx()
      val id = node.id
      val db_node = db.getNodeById(id)
      db_node.removeProperty(property)
      tx.success()
      tx.close()
      context.reply("delete property successfully!")
    }
    case UpdateNode(node, propertiesMap, oldLabel, newLabel) => {
      val tx = db.beginTx()
      val id = node.id
      val db_node = db.getNodeById(id)
      for (map <- propertiesMap) {
        db_node.setProperty(map._1, map._2)
      }
      if (oldLabel != "" && newLabel != "") {
        db_node.removeLabel(Label.label(oldLabel))
        db_node.addLabel(Label.label(newLabel))
      }
      if (oldLabel == "" && newLabel != "") {
        db_node.addLabel(Label.label(newLabel))
      }
      val driverNode = ValueConverter.toDriverNode(db_node)
      tx.success()
      tx.close()
      context.reply(driverNode)
    }

    case CreateRelationshipTo(node1, node2, relationship) => {
      val node1_id = node1.id
      val node2_id = node2.id
      val tx = db.beginTx()
      val dbNode1 = db.getNodeById(node1_id)
      val dbNode2 = db.getNodeById(node2_id)
      dbNode1.createRelationshipTo(dbNode2, RelationshipType.withName(relationship))
      tx.success()
      tx.close()
      context.reply("add relationship successfully!")
    }
    case GetNodeRelationships(node) => {
      val lst = ArrayBuffer[Relationship]()
      val tx = db.beginTx()
      val id = node.id
      val db_node = db.getNodeById(id)
      val relationships = db_node.getRelationships().iterator()
      while (relationships.hasNext) {
        val relation = relationships.next()
        val driverRelation = ValueConverter.toDriverRelationship(relation)
        lst += driverRelation
      }
      tx.success()
      tx.close()
      context.reply(lst)
    }
    case DeleteRelationship(node, relationship, direction) => {
      val tx = db.beginTx()
      val id = node.id
      val db_node = db.getNodeById(id)
      val relation = db_node.getSingleRelationship(RelationshipType.withName(relationship), direction)
      relation.delete()
      tx.success()
      tx.close()
      context.reply("delete relationship successfully!")
    }
    case GetAllNodes() => {
      //TODO:should use hippo stream to send big data
      val lst = ArrayBuffer[Node]()
      val tx = db.beginTx()
      val nodesIter = db.getAllNodes().iterator()
      while (nodesIter.hasNext) {
        lst += ValueConverter.toDriverNode(nodesIter.next())
      }
      tx.success()
      tx.close()
      context.reply(lst)
    }
    case GetAllRelationships() => {
      //TODO:should use hippo stream to send big data
      val lst = ArrayBuffer[Relationship]()
      val tx = db.beginTx()
      val relationships = db.getAllRelationships().iterator()
      while (relationships.hasNext) {
        lst += ValueConverter.toDriverRelationship(relationships.next())
      }
      tx.success()
      tx.close()
      context.reply(lst)
    }
  }
}

case class SayHelloRequest(msg: String)

case class SayHelloResponse(value: Any)

case class GetNodeById(id: Long)

case class GetNodesByProperty(label: String, propertiesMap: Map[String, Object])

case class CreateNode(label: String, propertiesMap: Map[String, Any])

case class DeleteNode(node: Node)

case class DeleteProperty(node: Node, property: String)

case class UpdateNode(node: Node, propertiesMap: Map[String, Any], oldLabel: String = "", newLabel: String = "")

case class CreateRelationshipTo(node1: Node, node2: Node, relationship: String)

case class GetNodeRelationships(node: Node)

case class DeleteRelationship(node: Node, relationship: String, direction: Direction)

case class GetAllNodes()

case class GetAllRelationships()

//TODO:
case class GetNodesByLabel(label: String)