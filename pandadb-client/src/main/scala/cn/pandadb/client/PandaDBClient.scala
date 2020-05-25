package cn.pandadb.client

import java.io.File

import cn.pandadb.blob.{BlobEntry, MimeType}
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnvClientConfig}
import net.neoremind.kraps.rpc.netty.HippoRpcEnvFactory
import cn.pandadb.datanode.DataNodeDriver
import cn.pandadb.leadernode.LeaderNodeDriver
import cn.pandadb.cluster.{ClusterInfoService, ZKTools}
import cn.pandadb.driver.result.InternalRecords
import cn.pandadb.util.PandaReplyMessage
import cn.pandadb.driver.values.Node
import org.apache.curator.shaded.com.google.common.net.HostAndPort

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration

class PandaDBClient(zkAddress: String) extends AutoCloseable{

  private val rpcServerName = "pandadb-client"
  private val LeaderNodeEndpointName = "leader-node-endpoint"
  private val DataNodeEndpointName = "data-node-endpoint"
  private val pandaDBZkDir = "/pandadb/vx.x.x/"

  private val zkTools = new ZKTools(zkAddress, pandaDBZkDir)
  private val clusterInfoService = new ClusterInfoService(zkTools)
  private val clientConfig = RpcEnvClientConfig(new RpcConf(), rpcServerName)
  private val clientRpcEnv = HippoRpcEnvFactory.create(clientConfig)

  private val leaderNode: HostAndPort = clusterInfoService.getLeaderNode()

  private val leaderNodeEndpointRef = clientRpcEnv.setupEndpointRef(
                  new RpcAddress(leaderNode.getHostText, leaderNode.getPort), LeaderNodeEndpointName)
  private val leaderNodeDriver = new LeaderNodeDriver

  private val dataNode: HostAndPort = clusterInfoService.randomGetDataNode()
//  println(dataNode)

  private val dataNodeEndpointRef = clientRpcEnv.setupEndpointRef(
                  new RpcAddress(dataNode.getHostText, dataNode.getPort), DataNodeEndpointName)

  private val dataNodeDriver = new DataNodeDriver

  /// update functions

  def createNode(labels: Array[String], properties: Map[String, Any]): Node = {
    val res = leaderNodeDriver.createNode(labels, properties, leaderNodeEndpointRef, Duration.Inf)
    res match {
      case n: Node => n
      case _ => throw new Exception("Return Type Invalid")
    }
  }

  def deleteNode(id: Long): PandaReplyMessage.Value = {
    leaderNodeDriver.deleteNode(id, leaderNodeEndpointRef, Duration.Inf)
  }

  def addNodeLabel(id: Long, label: String): PandaReplyMessage.Value = {
    leaderNodeDriver.addNodeLabel(id, label, leaderNodeEndpointRef, Duration.Inf)
  }

  //  def removeNodeLabel(id: Long, label: String): PandaReplyMessage.Value = {
  //    leaderNodeDriver.removeNodeLabel(id, label, leaderNodeEndpointRef, Duration.Inf)
  //  }

  def setNodeProperty(id: Long, propertiesMap: Map[String, Any]): PandaReplyMessage.Value = {
    leaderNodeDriver.updateNodeProperty(id: Long, propertiesMap: Map[String, Any], leaderNodeEndpointRef, Duration.Inf)
  }

  def removeNodeProperty(id: Long, property: String): PandaReplyMessage.Value = {
    leaderNodeDriver.removeProperty(id: Long, property, leaderNodeEndpointRef, Duration.Inf)
  }

  def createBlobFromFile(length: Long, mimeType: MimeType, file: File): BlobEntry = {
    leaderNodeDriver.createBlobEntry(length, mimeType, leaderNodeEndpointRef, Duration.Inf)
  }

  /// read functions

  def runCypher(cypher: String): InternalRecords = {
    dataNodeDriver.runCypher(cypher, dataNodeEndpointRef, Duration.Inf)
  }

  def getAllNodes(): Stream[Node] = {
    dataNodeDriver.getAllDBNodes(5, dataNodeEndpointRef, Duration.Inf)
  }

  def getNodeById(id: Long): Node = {
    dataNodeDriver.getNodeById(id, dataNodeEndpointRef, Duration.Inf)
  }

  def getNodesByLabel(label: String): ArrayBuffer[Node] = {
    dataNodeDriver.getNodesByLabel(label, dataNodeEndpointRef, Duration.Inf)
  }

  def findNodes(label: String, propertiesMap: Map[String, Any]): ArrayBuffer[Node] = {
    dataNodeDriver.getNodesByProperty(label, propertiesMap, dataNodeEndpointRef, Duration.Inf)
  }

  override def close(): Unit = {
    zkTools.close()
  }
}
