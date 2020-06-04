package cn.pandadb.leadernode

import java.io.File
import java.nio.ByteBuffer
import java.util.Random

import cn.pandadb.cluster.ClusterService
import cn.pandadb.configuration.{Config => PandaConfig}
import cn.pandadb.datanode.DataNodeServiceImpl
import cn.pandadb.driver.values.{Node, Relationship}
import cn.pandadb.util.PandaReplyMessage
import org.grapheco.hippo.{ChunkedStream, CompleteStream, HippoRpcHandler, ReceiveContext}
import org.neo4j.graphdb.GraphDatabaseService
import cn.pandadb.blob.storage.BlobStorageService
import cn.pandadb.index.IndexService
import cn.pandadb.store.local.DataStore
import org.slf4j.Logger

import scala.collection.mutable.ArrayBuffer

class LeaderNodeHandler(pandaConfig: PandaConfig, clusterService: ClusterService,
                        localNeo4jDB: GraphDatabaseService, blobStore: BlobStorageService,
                        indexService: IndexService, localDataStore: DataStore) extends HippoRpcHandler {
  val logger: Logger = pandaConfig.getLogger(this.getClass)

  val leaderNodeService = new LeaderNodeServiceImpl
  val dataNodeService = new DataNodeServiceImpl(localNeo4jDB)

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
    case GetZkDataNodes() => {
      val res = leaderNodeService.getZkDataNodes(clusterService)
      context.reply(res)
    }
    case GetLeaderDbFileNames() => {
      var fileNames = ArrayBuffer[String]()
//      val dbFile = new File(pandaConfig.getLocalNeo4jDatabasePath())
      val dbFile = localDataStore.graphStore
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
    case LeaderAddNodeLabel(id, label) => {
      val res = leaderNodeService.addNodeLabel(id, label, clusterService)
      val localResult = dataNodeService.addNodeLabel(id, label)

      if (res.equals(PandaReplyMessage.LEAD_NODE_SUCCESS) && localResult.equals(PandaReplyMessage.SUCCESS)) {
        context.reply(PandaReplyMessage.SUCCESS)
      } else {
        context.reply(PandaReplyMessage.FAILED)
      }
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
    case LeaderSetNodeProperty(id, propertiesMap) => {
      val res = leaderNodeService.setNodeProperty(id, propertiesMap, clusterService)
      val localResult = dataNodeService.setNodeProperty(id, propertiesMap)

      if (res.equals(PandaReplyMessage.LEAD_NODE_SUCCESS) && localResult.equals(PandaReplyMessage.SUCCESS)) {
        context.reply(PandaReplyMessage.SUCCESS)
      } else {
        context.reply(PandaReplyMessage.FAILED)
      }
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
    case LeaderRemoveNodeProperty(id, property) => {
      val res = leaderNodeService.removeNodeProperty(id, property, clusterService)
      val localResult = dataNodeService.removeNodeProperty(id, property)

      if (res.equals(PandaReplyMessage.LEAD_NODE_SUCCESS) && localResult.equals(PandaReplyMessage.SUCCESS)) {
        context.reply(PandaReplyMessage.SUCCESS)
      } else {
        context.reply(PandaReplyMessage.FAILED)
      }
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
    case LeaderDeleteNodeRelationship(startNodeId, endNodeId, relationshipName, direction) => {
      val res = leaderNodeService.deleteNodeRelationship(
        startNodeId, endNodeId, relationshipName, direction, clusterService
      )
      val localResult = dataNodeService.deleteNodeRelationship(startNodeId, endNodeId, relationshipName, direction)

      if (res.equals(PandaReplyMessage.LEAD_NODE_SUCCESS) && localResult.equals(PandaReplyMessage.SUCCESS)) {
        context.reply(PandaReplyMessage.SUCCESS)
      } else {
        context.reply(PandaReplyMessage.FAILED)
      }
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
    case LeaderCreateBlobEntry(length, mimeType) => {
      val bytes: Array[Byte] = new Array[Byte](extraInput.remaining())
      extraInput.get(bytes)
      val blobEntry = blobStore.save(length, mimeType, bytes)
      context.reply(blobEntry)
    }
  }

  override def openChunkedStream(): PartialFunction[Any, ChunkedStream] = {
    case LeaderSayHello(msg) => {
      null
    }
  }

  override def openCompleteStream(): PartialFunction[Any, CompleteStream] = {
    case LeaderSayHello(msg) => {
      null
    }
  }
}
