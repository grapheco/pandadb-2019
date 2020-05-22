package cn.pandadb.server

import java.io.{File, FileInputStream}
import java.nio.ByteBuffer
import java.util.Random

import cn.pandadb.blob.Blob
import cn.pandadb.blob.storage.BlobStorageService
import cn.pandadb.cluster.ClusterService
import org.grapheco.hippo.{ChunkedStream, CompleteStream, HippoRpcHandler, ReceiveContext}
import cn.pandadb.configuration.{Config => PandaConfig}
import cn.pandadb.datanode.{AddNodeLabel, CreateNode, CreateNodeRelationship, DataNodeServiceImpl, DeleteNode, DeleteNodeRelationship, DeleteRelationshipProperties, GetAllDBNodes, GetAllDBRelationships, GetNodeById, GetNodeRelationships, GetNodesByLabel, GetNodesByProperty, GetRelationshipByRelationId, ReadDbFileRequest, RemoveProperty, RunCypher, SayHello, UpdateNodeLabel, UpdateNodeProperty, UpdateRelationshipProperty}
import cn.pandadb.driver.values.Node
import cn.pandadb.leadernode.{GetLeaderDbFileNames, GetZkDataNodes, LeaderAddNodeLabel, LeaderCreateNode, LeaderCreateNodeRelationship, LeaderDeleteNode, LeaderDeleteNodeRelationship, LeaderDeleteRelationshipProperties, LeaderGetAllDBNodes, LeaderGetNodeById, LeaderGetNodeRelationships, LeaderGetNodesByLabel, LeaderGetNodesByProperty, LeaderGetRelationshipByRelationId, LeaderNodeServiceImpl, LeaderRemoveProperty, LeaderRunCypher, LeaderSayHello, LeaderUpdateNodeLabel, LeaderUpdateNodeProperty, LeaderUpdateRelationshipProperty}
import cn.pandadb.leadernode.{GetZkDataNodes, LeaderAddNodeLabel, LeaderCreateBlobEntry, LeaderCreateNode, LeaderCreateNodeRelationship, LeaderDeleteNode, LeaderDeleteNodeRelationship, LeaderDeleteRelationshipProperties, LeaderGetAllDBNodes, LeaderGetNodeById, LeaderGetNodeRelationships, LeaderGetNodesByLabel, LeaderGetNodesByProperty, LeaderGetRelationshipByRelationId, LeaderNodeServiceImpl, LeaderRemoveProperty, LeaderRunCypher, LeaderSayHello, LeaderUpdateNodeLabel, LeaderUpdateNodeProperty, LeaderUpdateRelationshipProperty}
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
    println(lst)
    lst
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
      val dataNodes = clusterService.getDataNodes()
      val nodeLength = dataNodes.size
      val chooseNumber = new Random().nextInt(nodeLength)
      val chooseNode = dataNodes(chooseNumber)
      if (chooseNode.equals(clusterService.getLeaderNode())) {
        val res = dataNodeService.runCypher(cypher)
        context.reply(res)
      } else {
        val strs = dataNodes(chooseNumber).split(":")
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
      val localResult = dataNodeService.createNodeLeader(labels, properties)
      val res = leaderNodeService.createNode(localResult.id, labels, properties, clusterService)

      if (res.equals(PandaReplyMessage.LEAD_NODE_SUCCESS) && localResult.isInstanceOf[Node]) {
        context.reply(PandaReplyMessage.SUCCESS)
      } else {
        context.reply(PandaReplyMessage.FAILED)
      }
    }
    case CreateNode(id, labels, properties) => {
      val driverNode = dataNodeService.createNodeFollow(id, labels, properties)
      context.reply(driverNode)
    }


    case LeaderDeleteNode(id) => {
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
      val dataNodes = clusterService.getDataNodes()
      val nodeLength = dataNodes.size
      val chooseNumber = new Random().nextInt(nodeLength)
      val chooseNode = dataNodes(chooseNumber)
      if (chooseNode.equals(clusterService.getLeaderNode())) {
        val res = dataNodeService.getNodeById(id)
        context.reply(res)
      } else {
        val strs = dataNodes(chooseNumber).split(":")
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
      val dataNodes = clusterService.getDataNodes()
      val nodeLength = dataNodes.size
      val chooseNumber = new Random().nextInt(nodeLength)
      val chooseNode = dataNodes(chooseNumber)
      if (chooseNode.equals(clusterService.getLeaderNode())) {
        val res = dataNodeService.getNodesByProperty(label, propertiesMap)
        context.reply(res)
      } else {
        val strs = dataNodes(chooseNumber).split(":")
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
      val dataNodes = clusterService.getDataNodes()
      val nodeLength = dataNodes.size
      val chooseNumber = new Random().nextInt(nodeLength)
      val chooseNode = dataNodes(chooseNumber)
      if (chooseNode.equals(clusterService.getLeaderNode())) {
        val res = dataNodeService.getNodesByLabel(label)
        context.reply(res)
      } else {
        val strs = dataNodes(chooseNumber).split(":")
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


    case LeaderUpdateNodeProperty(id, propertiesMap) => {
      val res = leaderNodeService.updateNodeProperty(id, propertiesMap, clusterService)
      val localResult = dataNodeService.updateNodeProperty(id, propertiesMap)
      if (res.equals(PandaReplyMessage.LEAD_NODE_SUCCESS) && localResult.equals(PandaReplyMessage.SUCCESS)) {
        context.reply(PandaReplyMessage.SUCCESS)
      } else {
        context.reply(PandaReplyMessage.FAILED)
      }
    }
    case UpdateNodeProperty(id, propertiesMap) => {
      val res = dataNodeService.updateNodeProperty(id, propertiesMap)
      context.reply(res)
    }


    case LeaderUpdateNodeLabel(id, toDeleteLabel, newLabel) => {
      val res = leaderNodeService.updateNodeLabel(id, toDeleteLabel, newLabel, clusterService)
      val localResult = dataNodeService.updateNodeLabel(id, toDeleteLabel, newLabel)
      if (res.equals(PandaReplyMessage.LEAD_NODE_SUCCESS) && localResult.equals(PandaReplyMessage.SUCCESS)) {
        context.reply(PandaReplyMessage.SUCCESS)
      } else {
        context.reply(PandaReplyMessage.FAILED)
      }
    }
    case UpdateNodeLabel(id, toDeleteLabel, newLabel) => {
      val res = dataNodeService.updateNodeLabel(id, toDeleteLabel, newLabel)
      context.reply(res)
    }


    case LeaderRemoveProperty(id, property) => {
      val res = leaderNodeService.removeProperty(id, property, clusterService)
      val localResult = dataNodeService.removeProperty(id, property)
      if (res.equals(PandaReplyMessage.LEAD_NODE_SUCCESS) && localResult.equals(PandaReplyMessage.SUCCESS)) {
        context.reply(PandaReplyMessage.SUCCESS)
      } else {
        context.reply(PandaReplyMessage.FAILED)
      }
    }
    case RemoveProperty(id, property) => {
      val res = dataNodeService.removeProperty(id, property)
      context.reply(res)
    }


    case LeaderCreateNodeRelationship(id1, id2, relationship, direction) => {
      val res = leaderNodeService.createNodeRelationship(id1, id2, relationship, direction, clusterService)
      val localResult = dataNodeService.createNodeRelationship(id1, id2, relationship, direction)
      if (res.equals(PandaReplyMessage.LEAD_NODE_SUCCESS) && localResult.equals(PandaReplyMessage.SUCCESS)) {
        context.reply(PandaReplyMessage.SUCCESS)
      } else {
        context.reply(PandaReplyMessage.FAILED)
      }
    }
    case CreateNodeRelationship(id1, id2, relationship, direction) => {
      val res = dataNodeService.createNodeRelationship(id1, id2, relationship, direction)
      context.reply(res)
    }


    case LeaderGetNodeRelationships(id) => {
      val dataNodes = clusterService.getDataNodes()
      val nodeLength = dataNodes.size
      val chooseNumber = new Random().nextInt(nodeLength)
      val chooseNode = dataNodes(chooseNumber)
      if (chooseNode.equals(clusterService.getLeaderNode())) {
        val res = dataNodeService.getNodeRelationships(id)
        context.reply(res)
      } else {
        val strs = dataNodes(chooseNumber).split(":")
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

    case LeaderDeleteNodeRelationship(id, relationship, direction) => {
      val res = leaderNodeService.deleteNodeRelationship(id, relationship, direction, clusterService)
      val localResult = dataNodeService.deleteNodeRelationship(id, relationship, direction)
      if (res.equals(PandaReplyMessage.LEAD_NODE_SUCCESS) && localResult.equals(PandaReplyMessage.SUCCESS)) {
        context.reply(PandaReplyMessage.SUCCESS)
      } else {
        context.reply(PandaReplyMessage.FAILED)
      }
      context.reply(res)
    }
    case DeleteNodeRelationship(id, relationship, direction) => {
      val res = dataNodeService.deleteNodeRelationship(id, relationship, direction)
      context.reply(res)
    }

    case LeaderGetRelationshipByRelationId(id) => {
      val dataNodes = clusterService.getDataNodes()
      val nodeLength = dataNodes.size
      val chooseNumber = new Random().nextInt(nodeLength)
      val chooseNode = dataNodes(chooseNumber)
      if (chooseNode.equals(clusterService.getLeaderNode())) {
        val res = dataNodeService.getRelationshipByRelationId(id)
        context.reply(res)
      } else {
        val strs = dataNodes(chooseNumber).split(":")
        val address = strs(0)
        val port = strs(1).toInt
        val res = leaderNodeService.getRelationshipByRelationId(id, address, port, clusterService)
        context.reply(res)
      }
    }
    case GetRelationshipByRelationId(id) => {
      val res = dataNodeService.getRelationshipByRelationId(id)
      context.reply(res)
    }


    case LeaderUpdateRelationshipProperty(id, propertyMap) => {
      val res = leaderNodeService.updateRelationshipProperty(id, propertyMap, clusterService)
      val localResult = dataNodeService.updateRelationshipProperty(id, propertyMap)
      if (res.equals(PandaReplyMessage.LEAD_NODE_SUCCESS) && localResult.equals(PandaReplyMessage.SUCCESS)) {
        context.reply(PandaReplyMessage.SUCCESS)
      } else {
        context.reply(PandaReplyMessage.FAILED)
      }
      context.reply(res)
    }
    case UpdateRelationshipProperty(id, propertyMap) => {
      val res = dataNodeService.updateRelationshipProperty(id, propertyMap)
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
    case ReadDbFileRequest(name) => {
      pandaConfig.getLocalNeo4jDatabasePath()
      val path = pandaConfig.getLocalNeo4jDatabasePath()
      dataNodeService.getDbFile(path, name)
    }
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
