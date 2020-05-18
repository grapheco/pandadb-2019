//package cn.pandadb.leadernode
//
//import java.io.File
//import java.nio.ByteBuffer
//
//import cn.pandadb.cluster.ClusterService
//import cn.pandadb.configuration.{Config => PandaConfig}
//import org.grapheco.hippo.{ChunkedStream, HippoRpcHandler, ReceiveContext}
//import org.neo4j.graphdb.GraphDatabaseService
//import org.neo4j.graphdb.factory.GraphDatabaseFactory
//import org.slf4j.Logger
//
//class LeaderNodeHandler(pandaConfig: PandaConfig, clusterService: ClusterService) extends HippoRpcHandler {
//  val logger: Logger = pandaConfig.getLogger(this.getClass)
//  val dbFile = new File(pandaConfig.getLocalNeo4jDatabasePath())
//  if (!dbFile.exists()) {
//    dbFile.mkdirs
//  }
//  val leaderNodeService = new LeaderNodeServiceImpl
////  var localNeo4jDB: GraphDatabaseService = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbFile).newGraphDatabase()
//
//  override def receiveWithBuffer(extraInput: ByteBuffer, context: ReceiveContext): PartialFunction[Any, Unit] = {
//    case LeaderSayHello(msg) => {
//      println("leader handler say hello")
//      val res = leaderNodeService.sayHello(clusterService)
//      context.reply(res)
//    }
//    case LeaderRunCypher(cypher) => {
//      val res = leaderNodeService.runCypher(cypher, clusterService)
//      context.reply(res)
//    }
//    case LeaderRunCypherOnAllNodes(cypher) => {
//      val res = leaderNodeService.runCypherOnAllNodes(cypher, clusterService)
//      context.reply(res)
//    }
//    case LeaderCreateNode(labels, properties) => {
//      val res = leaderNodeService.createNode(labels, properties, clusterService)
//      context.reply(res)
//    }
//    case LeaderDeleteNode(id) => {
//      val res = leaderNodeService.deleteNode(id, clusterService)
//      context.reply(res)
//    }
//    case LeaderAddNodeLabel(id, label) => {
//      val res = leaderNodeService.addNodeLabel(id, label, clusterService)
//      context.reply(res)
//    }
//    case LeaderGetNodeById(id) => {
//      val res = leaderNodeService.getNodeById(id, clusterService)
//      context.reply(res)
//    }
//    case LeaderGetNodesByProperty(label, propertiesMap) => {
//      val res = leaderNodeService.getNodesByProperty(label, propertiesMap, clusterService)
//      context.reply(res)
//    }
//    case LeaderGetNodesByLabel(label) => {
//      val res = leaderNodeService.getNodesByLabel(label, clusterService)
//      context.reply(res)
//    }
//    case LeaderUpdateNodeProperty(id, propertiesMap) => {
//      val res = leaderNodeService.updateNodeProperty(id, propertiesMap, clusterService)
//      context.reply(res)
//    }
//    case LeaderUpdateNodeLabel(id, toDeleteLabel, newLabel) => {
//      val res = leaderNodeService.updateNodeLabel(id, toDeleteLabel, newLabel, clusterService)
//      context.reply(res)
//    }
//    case LeaderRemoveProperty(id, property) => {
//      val res = leaderNodeService.removeProperty(id, property, clusterService)
//      context.reply(res)
//    }
//    case LeaderCreateNodeRelationship(id1, id2, relationship, direction) => {
//      val res = leaderNodeService.createNodeRelationship(id1, id2, relationship, direction, clusterService)
//      context.reply(res)
//    }
//    case LeaderGetNodeRelationships(id) => {
//      val res = leaderNodeService.getNodeRelationships(id, clusterService)
//      context.reply(res)
//    }
//    case LeaderDeleteNodeRelationship(id, relationship, direction) => {
//      val res = leaderNodeService.deleteNodeRelationship(id, relationship, direction, clusterService)
//      context.reply(res)
//    }
//    case GetZkDataNodes() => {
//      val res = leaderNodeService.getZkDataNodes(clusterService)
//      context.reply(res)
//    }
//  }
//
//  //  override def openChunkedStream(): PartialFunction[Any, ChunkedStream] = {
//  //    case LeaderGetAllDBNodes(chunkSize) => {
//  //      leaderNodeService.getAllDBNodes(localNeo4jDB, chunkSize)
//  //    }
//  //  }
//}
