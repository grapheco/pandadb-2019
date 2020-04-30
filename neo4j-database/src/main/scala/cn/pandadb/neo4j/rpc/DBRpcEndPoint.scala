package cn.pandadb.neo4j.rpc

import java.io.File
import java.nio.ByteBuffer

import cn.pandadb.neo4j.driver.values.{Node, Relationship}
import cn.pandadb.neo4j.util.{ServerReplyMsg, ValueConverter}
import net.neoremind.kraps.rpc._
import org.grapheco.hippo.{HippoRpcHandler, ReceiveContext}
import org.neo4j.graphdb.{GraphDatabaseService, Label, RelationshipType}
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.slf4j.Logger

import scala.collection.JavaConversions
import scala.collection.mutable.ArrayBuffer

class DBRpcEndpoint(override val rpcEnv: RpcEnv, log: Logger) extends RpcEndpoint with HippoRpcHandler {

  val dbFile = new File("./output/testdb")
  if (!dbFile.exists()) {
    dbFile.mkdirs
  }
  var db: GraphDatabaseService = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbFile).newGraphDatabase()

  override def onStart(): Unit = {
    log.info("start DBRpcEndpoint")
  }

  override def receiveWithBuffer(extraInput: ByteBuffer, context: ReceiveContext): PartialFunction[Any, Unit] = {
    case AddNode(labels, properties) => {
      val tx = db.beginTx()
      val node = db.createNode()
      for (labelName <- labels) {
        val label = Label.label(labelName)
        node.addLabel(label)
      }
      properties.foreach(x => {
        node.setProperty(x._1, x._2)
      })
      tx.success()
      tx.close()
      context.reply("Node id: " + node.getId)
    }
    case AddLabel(node, label) => {
      val tx = db.beginTx()
      val _node = db.getNodeById(node.id)
      _node.addLabel(Label.label(label))
      tx.success()
      tx.close()
      context.reply(ServerReplyMsg.SUCCESS)
    }
    case GetNodeById(id) => {
      val node = db.getNodeById(id)
      val driverNode = ValueConverter.toDriverNode(node)
      context.reply(driverNode)
    }
    case GetNodesByProperty(label, propertiesMap) => {
      val lst = ArrayBuffer[Node]()
      val tx = db.beginTx()
      val _label = Label.label(label)
      val propertyMaps = JavaConversions.mapAsJavaMap(propertiesMap)
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
    case GetNodesByLabel(label) => {
      val lst = ArrayBuffer[Node]()
      val tx = db.beginTx()
      val _label = Label.label(label)
      val res = db.findNodes(_label)
      while (res.hasNext) {
        val node = res.next()
        val driverNode = ValueConverter.toDriverNode(node)
        lst += driverNode
      }
      tx.success()
      tx.close()
      context.reply(lst)
    }
    case UpdateNodeProperty(node, propertiesMap) => {
      val tx = db.beginTx()
      val id = node.id
      val db_node = db.getNodeById(id)
      for (map <- propertiesMap) {
        db_node.setProperty(map._1, map._2)
      }
      tx.success()
      tx.close()
      context.reply(ServerReplyMsg.SUCCESS)
    }
    case UpdateNodeLabel(node, toDeleteLabel, newLabel) => {
      val tx = db.beginTx()
      val id = node.id
      val db_node = db.getNodeById(id)
      db_node.removeLabel(Label.label(toDeleteLabel))
      db_node.addLabel(Label.label(newLabel))
      tx.success()
      tx.close()
      context.reply(ServerReplyMsg.SUCCESS)
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
      context.reply(ServerReplyMsg.SUCCESS)
    }
    case RemoveProperty(node, property) => {
      val tx = db.beginTx()
      val id = node.id
      val db_node = db.getNodeById(id)
      db_node.removeProperty(property)
      tx.success()
      tx.close()
      context.reply(ServerReplyMsg.SUCCESS)
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
      context.reply(ServerReplyMsg.SUCCESS)
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
      context.reply(ServerReplyMsg.SUCCESS)
    }
    case GetAllDBNodes() => {
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
    case GetAllDBRelationships() => {
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

  override def onStop(): Unit = {
    log.info("stop DBRpcEndpoint")
  }
}