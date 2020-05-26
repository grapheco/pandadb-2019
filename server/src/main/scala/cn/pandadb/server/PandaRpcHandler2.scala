package cn.pandadb.server

import java.io.{File, FileInputStream}
import java.nio.ByteBuffer
import java.util.Random

import cn.pandadb.blob.Blob
import cn.pandadb.blob.storage.BlobStorageService
import cn.pandadb.cluster.ClusterService
import org.grapheco.hippo.{ChunkedStream, CompleteStream, HippoRpcHandler, ReceiveContext}
import cn.pandadb.configuration.{Config => PandaConfig}
import cn.pandadb.datanode.{AddNodeLabel, CreateNode, CreateNodeRelationship, DataNodeServiceImpl, DeleteNode, DeleteNodeRelationship, DeleteRelationshipProperties, GetAllDBLabels, GetAllDBNodes, GetAllDBRelationships, GetNodeById, GetNodeRelationships, GetNodesByLabel, GetNodesByProperty, GetRelationshipByRelationId, ReadDbFileRequest, RemoveNodeLabel, RemoveNodeProperty, RunCypher, SayHello, SetNodeProperty, SetRelationshipProperty}
import cn.pandadb.driver.values.{Node, Relationship}
import cn.pandadb.leadernode.{GetLeaderDbFileNames, GetZkDataNodes, LeaderAddNodeLabel, LeaderCreateBlobEntry, LeaderCreateNode, LeaderCreateNodeRelationship, LeaderDeleteNode, LeaderDeleteNodeRelationship, LeaderDeleteRelationshipProperties, LeaderGetNodeById, LeaderGetNodeRelationships, LeaderGetNodesByLabel, LeaderGetNodesByProperty, LeaderGetRelationshipByRelationId, LeaderNodeServiceImpl, LeaderRemoveNodeLabel, LeaderRemoveNodeProperty, LeaderRunCypher, LeaderSayHello, LeaderSetNodeProperty, LeaderSetRelationshipProperty}
import cn.pandadb.util.PandaReplyMessage
import io.netty.buffer.Unpooled
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.slf4j.Logger

import scala.collection.mutable.ArrayBuffer

class PandaRpcHandler2(pandaConfig: PandaConfig, clusterService: ClusterService, blobStore: BlobStorageService) extends HippoRpcHandler {
  val logger: Logger = pandaConfig.getLogger(this.getClass)
  val dbFile = new File(pandaConfig.getLocalNeo4jDatabasePath())
  if (!dbFile.exists()) {
    dbFile.mkdirs
  }
  var localNeo4jDB: GraphDatabaseService = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbFile).newGraphDatabase()
  val dataNodeService = new DataNodeServiceImpl(localNeo4jDB)
  val leaderNodeService = new LeaderNodeServiceImpl


  def readDbFileNames(dir: File, lst: ArrayBuffer[String], currentPath: String = ""): ArrayBuffer[String] = {
    dir.listFiles.map(file => {
      if (file.isDirectory) {
        if (currentPath != "") {
          val moreCurrentPath = currentPath + file.getName + "/"
          readDbFileNames(file, lst, moreCurrentPath)
        } else {
          val moreFile = file.getName + "/"
          readDbFileNames(file, lst, moreFile)
        }
      }
      else {
        if (currentPath == "") {
          lst += file.getName
        } else {
          val temp = currentPath + file.getName
          lst += temp
        }
      }
    })
    lst
  }

  def chooseNodeFromZk(): String = {
    val dataNodes = clusterService.getDataNodes()
    val nodeLength = dataNodes.size
    val chooseNumber = new Random().nextInt(nodeLength)
    dataNodes(chooseNumber)
  }

  override def receiveWithBuffer(extraInput: ByteBuffer, context: ReceiveContext): PartialFunction[Any, Unit] = {
    // leader's func
    case GetZkDataNodes() => {
      val res = leaderNodeService.getZkDataNodes(clusterService)
      context.reply(res)
    }
    // leader's func
    case GetLeaderDbFileNames() => {
      var fileNames = ArrayBuffer[String]()
      fileNames = readDbFileNames(dbFile, fileNames)
      context.reply(fileNames)
    }

    case LeaderSayHello(msg) => {
      val res = leaderNodeService.sayHello(clusterService)
      val localResult = dataNodeService.sayHello("hello")

      if (res.equals(PandaReplyMessage.LEAD_NODE_SUCCESS) && localResult.equals(PandaReplyMessage.SUCCESS)) {
        context.reply(PandaReplyMessage.SUCCESS)
      } else {
        context.reply(PandaReplyMessage.FAILED)
      }
    }
    case SayHello(msg) => {
      val res = dataNodeService.sayHello(msg)
      context.reply(res)
    }


    case LeaderRunCypher(cypher) => {
      // random choose a node in ZK, if leader use local func, else rpc to other node
      val chooseNode = chooseNodeFromZk()
      if (chooseNode.equals(clusterService.getLeaderNode())) {
        val res = dataNodeService.runCypher(cypher)
        context.reply(res)
      } else {
        val strs = chooseNode.split(":")
        val address = strs(0)
        val port = strs(1).toInt
        val res = leaderNodeService.runCypher(cypher, address, port, clusterService)
        context.reply(res)
      }
    }
    case RunCypher(cypher) => {
      val res = dataNodeService.runCypher(cypher)
      context.reply(res)
    }


    case LeaderCreateNode(labels, properties) => {
      // first local create and get node id then followers use this id to create node
      val localResult = dataNodeService.createNodeLeader(labels, properties)
      val res = leaderNodeService.createNode(localResult.id, labels, properties, clusterService)

      if (res.equals(PandaReplyMessage.LEAD_NODE_SUCCESS) && localResult.isInstanceOf[Node]) {
        context.reply(localResult)
      } else {
        context.reply(PandaReplyMessage.FAILED)
      }
    }
    case CreateNode(id, labels, properties) => {
      val res = dataNodeService.createNodeFollower(id, labels, properties)
      context.reply(res)
    }


    case LeaderCreateNodeRelationship(id1, id2, relationship, direction) => {
      // same as create node
      val localResult = dataNodeService.createNodeRelationshipLeader(id1, id2, relationship, direction)
      val relationId = localResult.map(r => r.id)
      val res = leaderNodeService.createNodeRelationship(relationId, id1, id2, relationship, direction, clusterService)

      if (res.equals(PandaReplyMessage.LEAD_NODE_SUCCESS) && localResult.isInstanceOf[ArrayBuffer[Relationship]]) {
        context.reply(localResult)
      } else {
        context.reply(PandaReplyMessage.FAILED)
      }
    }
    case CreateNodeRelationship(rId, id1, id2, relationship, direction) => {
      val res = dataNodeService.createNodeRelationshipFollower(rId, id1, id2, relationship, direction)
      context.reply(res)
    }


    case LeaderDeleteNode(id) => {
      // first delete remote nodes, if failed just handle them
      val res = leaderNodeService.deleteNode(id, clusterService)
      val localResult = dataNodeService.deleteNode(id)

      if (res.equals(PandaReplyMessage.LEAD_NODE_SUCCESS) && localResult.equals(PandaReplyMessage.SUCCESS)) {
        context.reply(PandaReplyMessage.SUCCESS)
      } else {
        context.reply(PandaReplyMessage.FAILED)
      }
    }
    case DeleteNode(id) => {
      val res = dataNodeService.deleteNode(id)
      context.reply(res)
    }


    case LeaderAddNodeLabel(id, label) => {
      val res = leaderNodeService.addNodeLabel(id, label, clusterService)
      val localResult = dataNodeService.addNodeLabel(id, label)

      if (res.equals(PandaReplyMessage.LEAD_NODE_SUCCESS) && localResult.equals(PandaReplyMessage.SUCCESS)) {
        context.reply(PandaReplyMessage.SUCCESS)
      } else {
        context.reply(PandaReplyMessage.FAILED)
      }
    }
    case AddNodeLabel(id, label) => {
      val res = dataNodeService.addNodeLabel(id, label)
      context.reply(res)
    }


    case LeaderGetNodeById(id) => {
      val chooseNode = chooseNodeFromZk()
      if (chooseNode.equals(clusterService.getLeaderNode())) {
        val res = dataNodeService.getNodeById(id)
        context.reply(res)
      } else {
        val strs = chooseNode.split(":")
        val address = strs(0)
        val port = strs(1).toInt
        val res = leaderNodeService.getNodeById(id, address, port, clusterService)
        context.reply(res)
      }
    }
    case GetNodeById(id) => {
      val node = dataNodeService.getNodeById(id)
      context.reply(node)
    }


    case LeaderGetNodesByProperty(label, propertiesMap) => {
      val chooseNode = chooseNodeFromZk()
      if (chooseNode.equals(clusterService.getLeaderNode())) {
        val res = dataNodeService.getNodesByProperty(label, propertiesMap)
        context.reply(res)
      } else {
        val strs = chooseNode.split(":")
        val address = strs(0)
        val port = strs(1).toInt
        val res = leaderNodeService.getNodesByProperty(label, address, port, propertiesMap, clusterService)
        context.reply(res)
      }
    }
    case GetNodesByProperty(label, propertiesMap) => {
      //maybe to chunkStream
      val res = dataNodeService.getNodesByProperty(label, propertiesMap)
      context.reply(res)
    }


    case LeaderGetNodesByLabel(label) => {
      val chooseNode = chooseNodeFromZk()
      if (chooseNode.equals(clusterService.getLeaderNode())) {
        val res = dataNodeService.getNodesByLabel(label)
        context.reply(res)
      } else {
        val strs = chooseNode.split(":")
        val address = strs(0)
        val port = strs(1).toInt
        val res = leaderNodeService.getNodesByLabel(label, address, port, clusterService)
        context.reply(res)
      }
    }
    case GetNodesByLabel(label) => {
      val res = dataNodeService.getNodesByLabel(label)
      context.reply(res)
    }


    case LeaderSetNodeProperty(id, propertiesMap) => {
      val res = leaderNodeService.setNodeProperty(id, propertiesMap, clusterService)
      val localResult = dataNodeService.setNodeProperty(id, propertiesMap)

      if (res.equals(PandaReplyMessage.LEAD_NODE_SUCCESS) && localResult.equals(PandaReplyMessage.SUCCESS)) {
        context.reply(PandaReplyMessage.SUCCESS)
      } else {
        context.reply(PandaReplyMessage.FAILED)
      }
    }
    case SetNodeProperty(id, propertiesMap) => {
      val res = dataNodeService.setNodeProperty(id, propertiesMap)
      context.reply(res)
    }


    case LeaderRemoveNodeLabel(id, toDeleteLabel) => {
      val res = leaderNodeService.removeNodeLabel(id, toDeleteLabel, clusterService)
      val localResult = dataNodeService.removeNodeLabel(id, toDeleteLabel)

      if (res.equals(PandaReplyMessage.LEAD_NODE_SUCCESS) && localResult.equals(PandaReplyMessage.SUCCESS)) {
        context.reply(PandaReplyMessage.SUCCESS)
      } else {
        context.reply(PandaReplyMessage.FAILED)
      }
    }
    case RemoveNodeLabel(id, toDeleteLabel) => {
      val res = dataNodeService.removeNodeLabel(id, toDeleteLabel)
      context.reply(res)
    }


    case LeaderRemoveNodeProperty(id, property) => {
      val res = leaderNodeService.removeNodeProperty(id, property, clusterService)
      val localResult = dataNodeService.removeNodeProperty(id, property)

      if (res.equals(PandaReplyMessage.LEAD_NODE_SUCCESS) && localResult.equals(PandaReplyMessage.SUCCESS)) {
        context.reply(PandaReplyMessage.SUCCESS)
      } else {
        context.reply(PandaReplyMessage.FAILED)
      }
    }
    case RemoveNodeProperty(id, property) => {
      val res = dataNodeService.removeNodeProperty(id, property)
      context.reply(res)
    }

    case LeaderGetNodeRelationships(id) => {
      val chooseNode = chooseNodeFromZk()
      if (chooseNode.equals(clusterService.getLeaderNode())) {
        val res = dataNodeService.getNodeRelationships(id)
        context.reply(res)
      } else {
        val strs = chooseNode.split(":")
        val address = strs(0)
        val port = strs(1).toInt
        val res = leaderNodeService.getNodeRelationships(id, address, port, clusterService)
        context.reply(res)
      }
    }
    case GetNodeRelationships(id) => {
      val res = dataNodeService.getNodeRelationships(id)
      context.reply(res)
    }

    case LeaderDeleteNodeRelationship(startNodeId, endNodeId, relationshipName, direction) => {
      val res = leaderNodeService.deleteNodeRelationship(startNodeId, endNodeId, relationshipName, direction, clusterService)
      val localResult = dataNodeService.deleteNodeRelationship(startNodeId, endNodeId, relationshipName, direction)

      if (res.equals(PandaReplyMessage.LEAD_NODE_SUCCESS) && localResult.equals(PandaReplyMessage.SUCCESS)) {
        context.reply(PandaReplyMessage.SUCCESS)
      } else {
        context.reply(PandaReplyMessage.FAILED)
      }
      context.reply(res)
    }
    case DeleteNodeRelationship(startNodeId, endNodeId, relationshipName, direction) => {
      val res = dataNodeService.deleteNodeRelationship(startNodeId, endNodeId, relationshipName, direction)
      context.reply(res)
    }

    case LeaderGetRelationshipByRelationId(id) => {
      val chooseNode = chooseNodeFromZk()
      if (chooseNode.equals(clusterService.getLeaderNode())) {
        val res = dataNodeService.getRelationshipById(id)
        context.reply(res)
      } else {
        val strs = chooseNode.split(":")
        val address = strs(0)
        val port = strs(1).toInt
        val res = leaderNodeService.getRelationshipByRelationId(id, address, port, clusterService)
        context.reply(res)
      }
    }
    case GetRelationshipByRelationId(id) => {
      val res = dataNodeService.getRelationshipById(id)
      context.reply(res)
    }


    case LeaderSetRelationshipProperty(id, propertyMap) => {
      val res = leaderNodeService.setRelationshipProperty(id, propertyMap, clusterService)
      val localResult = dataNodeService.setRelationshipProperty(id, propertyMap)

      if (res.equals(PandaReplyMessage.LEAD_NODE_SUCCESS) && localResult.equals(PandaReplyMessage.SUCCESS)) {
        context.reply(PandaReplyMessage.SUCCESS)
      } else {
        context.reply(PandaReplyMessage.FAILED)
      }
      context.reply(res)
    }
    case SetRelationshipProperty(id, propertyMap) => {
      val res = dataNodeService.setRelationshipProperty(id, propertyMap)
      context.reply(res)
    }


    case LeaderDeleteRelationshipProperties(id, propertyArray) => {
      val res = leaderNodeService.deleteRelationshipProperties(id, propertyArray, clusterService)
      val localResult = dataNodeService.deleteRelationshipProperties(id, propertyArray)

      if (res.equals(PandaReplyMessage.LEAD_NODE_SUCCESS) && localResult.equals(PandaReplyMessage.SUCCESS)) {
        context.reply(PandaReplyMessage.SUCCESS)
      } else {
        context.reply(PandaReplyMessage.FAILED)
      }
      context.reply(res)
    }
    case DeleteRelationshipProperties(id, propertyArray) => {
      val res = dataNodeService.deleteRelationshipProperties(id, propertyArray)
      context.reply(res)
    }

    case LeaderCreateBlobEntry(length, mimeType) => {
      val blobEntry = blobStore.save(length, mimeType, null)
      context.reply(blobEntry)
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
    //    case ReadDbFileRequest(name) => {
    //      pandaConfig.getLocalNeo4jDatabasePath()
    //      val path = pandaConfig.getLocalNeo4jDatabasePath()
    //      dataNodeService.getDbFile(path, name)
    //    }
  }

  override def openCompleteStream(): PartialFunction[Any, CompleteStream] = {
    case ReadDbFileRequest(name) => {
      val path = pandaConfig.getLocalNeo4jDatabasePath()
      val filePath = path + "/" + name
      val fis = new FileInputStream(new File(filePath))
      val buf = Unpooled.buffer()
      buf.writeBytes(fis.getChannel, new File(filePath).length().toInt)
      CompleteStream.fromByteBuffer(buf);
    }
  }
}
