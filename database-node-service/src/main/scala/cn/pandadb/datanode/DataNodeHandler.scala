//package cn.pandadb.datanode
//
//import java.io.File
//import java.nio.ByteBuffer
//
//import org.neo4j.graphdb.GraphDatabaseService
//import org.neo4j.graphdb.factory.GraphDatabaseFactory
//import org.slf4j.Logger
//import cn.pandadb.configuration.{Config => PandaConfig}
//import cn.pandadb.util.PandaReplyMessage
//import org.grapheco.hippo.{ChunkedStream, HippoRpcHandler, ReceiveContext}
//
//class DataNodeHandler(pandaConfig: PandaConfig) extends HippoRpcHandler {
//  val logger: Logger = pandaConfig.getLogger(this.getClass)
//  val dbFile = new File(pandaConfig.getLocalNeo4jDatabasePath())
//  if (!dbFile.exists()) {
//    dbFile.mkdirs
//  }
//  var localNeo4jDB: GraphDatabaseService = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbFile).newGraphDatabase()
//  val dataNodeService = new DataNodeServiceImpl(localNeo4jDB)
//
//  override def openChunkedStream(): PartialFunction[Any, ChunkedStream] = {
//    case GetAllDBNodes(chunkSize) => {
//      dataNodeService.getAllDBNodes(chunkSize)
//    }
//    case GetAllDBRelationships(chunkSize) => {
//      dataNodeService.getAllDBRelationships(chunkSize)
//    }
//  }
//
//  override def receiveWithBuffer(extraInput: ByteBuffer, context: ReceiveContext): PartialFunction[Any, Unit] = {
//    case RunCypher(cypher) => {
//      println(cypher)
//      val res = dataNodeService.runCypher(cypher)
//      context.reply(res)
//    }
//
//    case SayHello(msg) => {
//      println("data node handler say hello")
//      context.reply(PandaReplyMessage.SUCCESS)
//    }
//    case CreateNode(id, labels, properties) => {
//      val driverNode = dataNodeService.createNodeFollow(id, labels, properties)
//      context.reply(driverNode)
//    }
//    case AddNodeLabel(id, label) => {
//      val driverNode = dataNodeService.addNodeLabel(id, label)
//      context.reply(PandaReplyMessage.SUCCESS)
//    }
//    case GetNodeById(id) => {
//      val node = dataNodeService.getNodeById(id)
//      context.reply(node)
//    }
//    case GetNodesByProperty(label, propertiesMap) => {
//      //maybe to chunkStream
//      val res = dataNodeService.getNodesByProperty(label, propertiesMap)
//      context.reply(res)
//    }
//    case GetNodesByLabel(label) => {
//      val res = dataNodeService.getNodesByLabel(label)
//      context.reply(res)
//    }
//    case UpdateNodeProperty(id, propertiesMap) => {
//      val res = dataNodeService.updateNodeProperty(id, propertiesMap)
//      context.reply(res)
//    }
//    case UpdateNodeLabel(id, toDeleteLabel, newLabel) => {
//      val res = dataNodeService.updateNodeLabel(id, toDeleteLabel, newLabel)
//      context.reply(res)
//    }
//    case DeleteNode(id) => {
//      val res = dataNodeService.deleteNode(id)
//      context.reply(res)
//    }
//    case RemoveProperty(id, property) => {
//      val res = dataNodeService.removeProperty(id, property)
//      context.reply(res)
//    }
//    case CreateNodeRelationship(id1, id2, relationship, direction) => {
//      val res = dataNodeService.createNodeRelationship(id1, id2, relationship, direction)
//      context.reply(res)
//    }
//    case GetNodeRelationships(id) => {
//      val res = dataNodeService.getNodeRelationships(id)
//      context.reply(res)
//    }
//    case DeleteNodeRelationship(id, relationship, direction) => {
//      val res = dataNodeService.deleteNodeRelationship(id, relationship, direction)
//      context.reply(res)
//    }
//  }
//}
