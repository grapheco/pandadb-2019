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

class PandaRpcHandler(pandaConfig: PandaConfig, clusterService: ClusterService) extends HippoRpcHandler {
  val lst = ArrayBuffer[HippoRpcHandler]()

  def add(handler: HippoRpcHandler): Unit = {
    lst += handler
  }

  override def receiveWithBuffer(extraInput: ByteBuffer, context: ReceiveContext): PartialFunction[Any, Unit] = {
    case message => {
      val res = lst.filter(_.receiveWithBuffer(extraInput, context).isDefinedAt(message))
      res(0).receiveWithBuffer(extraInput, context).apply(message)
    }
  }

  override def openCompleteStream(): PartialFunction[Any, CompleteStream] = {
    case message => {
      val res = lst.filter(_.openCompleteStream().isDefinedAt(message))
      res(0).openCompleteStream().apply(message)
    }
  }

  override def openChunkedStream(): PartialFunction[Any, ChunkedStream] = {
    case message => {
      val res = lst.filter(_.openChunkedStream().isDefinedAt(message))
      res(0).openChunkedStream().apply(message)
    }
  }
}
