package cn.pandadb.datanode

import java.io.{File, FileInputStream}
import java.nio.ByteBuffer

import cn.pandadb.blob.storage.BlobStorageService
import org.neo4j.graphdb.GraphDatabaseService
import org.slf4j.Logger
import cn.pandadb.configuration.{Config => PandaConfig}
import cn.pandadb.index.IndexService
import cn.pandadb.util.CompressDbFileUtil
import io.netty.buffer.Unpooled
import org.grapheco.hippo.{ChunkedStream, CompleteStream, HippoRpcHandler, ReceiveContext}
import cn.pandadb.store.local.DataStore

class DataNodeHandler(pandaConfig: PandaConfig,
                      localNeo4jDB: GraphDatabaseService, blobStore: BlobStorageService,
                      indexService: IndexService, localDataStore: DataStore) extends HippoRpcHandler {
  val logger: Logger = pandaConfig.getLogger(this.getClass)

  val dataNodeService = new DataNodeServiceImpl(localNeo4jDB)

  override def receiveWithBuffer(extraInput: ByteBuffer, context: ReceiveContext): PartialFunction[Any, Unit] = {
    case SayHello(msg) => {
      val res = dataNodeService.sayHello(msg)
      context.reply(res)
    }
    case RunCypher(cypher) => {
      val res = dataNodeService.runCypher(cypher)
      context.reply(res)
    }
    case CreateNode(id, labels, properties) => {
      val res = dataNodeService.createNodeFollower(id, labels, properties)
      context.reply(res)
    }
    case CreateNodeRelationship(rId, id1, id2, relationship, direction) => {
      val res = dataNodeService.createNodeRelationshipFollower(rId, id1, id2, relationship, direction)
      context.reply(res)
    }
    case DeleteNode(id) => {
      val res = dataNodeService.deleteNode(id)
      context.reply(res)
    }
    case AddNodeLabel(id, label) => {
      val res = dataNodeService.addNodeLabel(id, label)
      context.reply(res)
    }
    case GetNodeById(id) => {
      val node = dataNodeService.getNodeById(id)
      context.reply(node)
    }
    case GetNodesByProperty(label, propertiesMap) => {
      //maybe to chunkStream
      val res = dataNodeService.getNodesByProperty(label, propertiesMap)
      context.reply(res)
    }
    case GetNodesByLabel(label) => {
      val res = dataNodeService.getNodesByLabel(label)
      context.reply(res)
    }
    case SetNodeProperty(id, propertiesMap) => {
      val res = dataNodeService.setNodeProperty(id, propertiesMap)
      context.reply(res)
    }
    case RemoveNodeLabel(id, toDeleteLabel) => {
      val res = dataNodeService.removeNodeLabel(id, toDeleteLabel)
      context.reply(res)
    }
    case RemoveNodeProperty(id, property) => {
      val res = dataNodeService.removeNodeProperty(id, property)
      context.reply(res)
    }
    case GetNodeRelationships(id) => {
      val res = dataNodeService.getNodeRelationships(id)
      context.reply(res)
    }
    case DeleteNodeRelationship(startNodeId, endNodeId, relationshipName, direction) => {
      val res = dataNodeService.deleteNodeRelationship(startNodeId, endNodeId, relationshipName, direction)
      context.reply(res)
    }
    case GetRelationshipByRelationId(id) => {
      val res = dataNodeService.getRelationshipById(id)
      context.reply(res)
    }
    case SetRelationshipProperty(id, propertyMap) => {
      val res = dataNodeService.setRelationshipProperty(id, propertyMap)
      context.reply(res)
    }
    case DeleteRelationshipProperties(id, propertyArray) => {
      val res = dataNodeService.deleteRelationshipProperties(id, propertyArray)
      context.reply(res)
    }

  }

  override def openChunkedStream(): PartialFunction[Any, ChunkedStream] = {
    case GetAllDBNodes(chunkSize) => {
      dataNodeService.getAllDBNodes(chunkSize)
    }
    case GetAllDBRelationships(chunkSize) => {
      dataNodeService.getAllDBRelationships(chunkSize)
    }
    case GetAllDBLabels(chunkSize) => {
      dataNodeService.getAllDBLabels(chunkSize)
    }
  }

  override def openCompleteStream(): PartialFunction[Any, CompleteStream] = {
    case ReadDbFileRequest(name) => {
      //      val path = pandaConfig.getLocalNeo4jDatabasePath()
      val path = localDataStore.graphStore.getAbsolutePath
      val filePath = path + File.separator + name
      val fis = new FileInputStream(new File(filePath))
      val buf = Unpooled.buffer()
      buf.writeBytes(fis.getChannel, new File(filePath).length().toInt)
      CompleteStream.fromByteBuffer(buf);
    }
    case ReadCompressedDbFileRequest(zipFileName) => {
      val util = new CompressDbFileUtil
      //      val path = pandaConfig.getLocalNeo4jDatabasePath()
      val path = localDataStore.graphStore.getAbsolutePath
      val toCompressPath = Map("graphStore" -> localDataStore.graphStore.getAbsolutePath,
        "dataVersionStore" -> localDataStore.dataVersionStore.getAbsolutePath)
      val compressToDir = path.substring(0, path.lastIndexOf(File.separator)) + "/compress/"
      util.compressToZip(toCompressPath, compressToDir, zipFileName)

      val filePath = compressToDir + zipFileName
      val fis = new FileInputStream(new File(filePath))
      val buf = Unpooled.buffer()
      buf.writeBytes(fis.getChannel, new File(filePath).length().toInt)
      CompleteStream.fromByteBuffer(buf);
    }
  }
}
