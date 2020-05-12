package cn.pandadb.server

import java.io.File
import java.nio.ByteBuffer

import cn.pandadb.cluster.ClusterService
import cn.pandadb.datanode.{AddNodeLabel, CreateNode, CreateNodeRelationship, DataNodeHandler, DataNodeServiceImpl, DeleteNode, DeleteNodeRelationship, GetAllDBNodes, GetAllDBRelationships, GetNodeById, GetNodeRelationships, GetNodesByLabel, GetNodesByProperty, RemoveProperty, RunCypher, SayHello, UpdateNodeLabel, UpdateNodeProperty}
import org.grapheco.hippo.{ChunkedStream, HippoRpcHandler, ReceiveContext}
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.slf4j.Logger
import cn.pandadb.configuration.{Config => PandaConfig}
import cn.pandadb.leadernode.{LeaderAddNodeLabel, LeaderCreateNode, LeaderCreateNodeRelationship, LeaderDeleteNode, LeaderDeleteNodeRelationship, LeaderGetAllDBNodes, LeaderGetNodeById, LeaderGetNodeRelationships, LeaderGetNodesByLabel, LeaderGetNodesByProperty, LeaderNodeServiceImpl, LeaderRemoveProperty, LeaderRunCypher, LeaderSayHello, LeaderUpdateNodeLabel, LeaderUpdateNodeProperty}
import cn.pandadb.util.PandaReplyMsg

class PandaRpcHandler(pandaConfig: PandaConfig, clusterService: ClusterService) extends HippoRpcHandler {
  val logger: Logger = pandaConfig.getLogger(this.getClass)
  val dbFile = new File(pandaConfig.getLocalNeo4jDatabasePath())
  if (!dbFile.exists()) {
    dbFile.mkdirs
  }
  var localNeo4jDB: GraphDatabaseService = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbFile).newGraphDatabase()
  val dataNodeService = new DataNodeServiceImpl(localNeo4jDB)
  val leaderNodeService = new LeaderNodeServiceImpl


  override def openChunkedStream(): PartialFunction[Any, ChunkedStream] = {
    case GetAllDBNodes(chunkSize) => {
      dataNodeService.getAllDBNodes(chunkSize)
    }

    case GetAllDBRelationships(chunkSize) => {
      dataNodeService.getAllDBRelationships(chunkSize)
    }

    //    case LeaderGetAllDBNodes(chunkSize) => {
    //      leaderNodeService.getAllDBNodes(localNeo4jDB, chunkSize)
    //    }
  }

  override def receiveWithBuffer(extraInput: ByteBuffer, context: ReceiveContext): PartialFunction[Any, Unit] = {

    case LeaderSayHello(msg) => {
      val res = leaderNodeService.sayHello(clusterService)
      context.reply(res)
    }
    case SayHello(msg) => {
      context.reply(PandaReplyMsg.SUCCESS)
    }


    case RunCypher(cypher) => {
      println(cypher)
      val res = dataNodeService.runCypher(cypher)
      context.reply(res)
    }
    case LeaderRunCypher(cypher) => {
      val res = leaderNodeService.runCypher(cypher, clusterService)
      context.reply(res)
    }


    case CreateNode(labels, properties) => {
      val driverNode = dataNodeService.createNode(labels, properties)
      context.reply(driverNode)
    }
    case LeaderCreateNode(labels, properties) => {
      val res = leaderNodeService.createNode(labels, properties, clusterService)
      context.reply(res)
    }


    case AddNodeLabel(id, label) => {
      val driverNode = dataNodeService.addNodeLabel(id, label)
      context.reply(PandaReplyMsg.SUCCESS)
    }
    case LeaderAddNodeLabel(id, label) => {
      val res = leaderNodeService.addNodeLabel(id, label, clusterService)
      context.reply(res)
    }


    case GetNodeById(id) => {
      val node = dataNodeService.getNodeById(id)
      context.reply(node)
    }
    case LeaderGetNodeById(id) => {
      val res = leaderNodeService.getNodeById(id, clusterService)
      context.reply(res)
    }


    case GetNodesByProperty(label, propertiesMap) => {
      //maybe to chunkStream
      val res = dataNodeService.getNodesByProperty(label, propertiesMap)
      context.reply(res)
    }
    case LeaderGetNodesByProperty(label, propertiesMap) => {
      val res = leaderNodeService.getNodesByProperty(label, propertiesMap, clusterService)
      context.reply(res)
    }


    case GetNodesByLabel(label) => {
      val res = dataNodeService.getNodesByLabel(label)
      context.reply(res)
    }
    case LeaderGetNodesByLabel(label) => {
      val res = leaderNodeService.getNodesByLabel(label, clusterService)
      context.reply(res)
    }


    case UpdateNodeProperty(id, propertiesMap) => {
      val res = dataNodeService.updateNodeProperty(id, propertiesMap)
      context.reply(res)
    }
    case LeaderUpdateNodeProperty(id, propertiesMap) => {
      val res = leaderNodeService.updateNodeProperty(id, propertiesMap, clusterService)
      context.reply(res)
    }


    case UpdateNodeLabel(id, toDeleteLabel, newLabel) => {
      val res = dataNodeService.updateNodeLabel(id, toDeleteLabel, newLabel)
      context.reply(res)
    }
    case LeaderUpdateNodeLabel(id, toDeleteLabel, newLabel) => {
      val res = leaderNodeService.updateNodeLabel(id, toDeleteLabel, newLabel, clusterService)
      context.reply(res)
    }


    case DeleteNode(id) => {
      val res = dataNodeService.deleteNode(id)
      context.reply(res)
    }
    case LeaderDeleteNode(id) => {
      val res = leaderNodeService.deleteNode(id, clusterService)
      context.reply(res)
    }


    case RemoveProperty(id, property) => {
      val res = dataNodeService.removeProperty(id, property)
      context.reply(res)
    }
    case LeaderRemoveProperty(id, property) => {
      val res = leaderNodeService.removeProperty(id, property, clusterService)
      context.reply(res)
    }


    case CreateNodeRelationship(id1, id2, relationship, direction) => {
      val res = dataNodeService.createNodeRelationship(id1, id2, relationship, direction)
      context.reply(res)
    }
    case LeaderCreateNodeRelationship(id1, id2, relationship, direction) => {
      val res = leaderNodeService.createNodeRelationship(id1, id2, relationship, direction, clusterService)
      context.reply(res)
    }


    case GetNodeRelationships(id) => {
      val res = dataNodeService.getNodeRelationships(id)
      context.reply(res)
    }
    case LeaderGetNodeRelationships(id) => {
      val res = leaderNodeService.getNodeRelationships(id, clusterService)
      context.reply(res)
    }

    case DeleteNodeRelationship(id, relationship, direction) => {
      val res = dataNodeService.deleteNodeRelationship(id, relationship, direction)
      context.reply(res)
    }
    case LeaderDeleteNodeRelationship(id, relationship, direction) => {
      val res = leaderNodeService.deleteNodeRelationship(id, relationship, direction, clusterService)
      context.reply(res)
    }

  }
}
