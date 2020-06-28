package cn.pandadb.leadernode

import cn.pandadb.cluster.ClusterService
import cn.pandadb.configuration.Config
import cn.pandadb.datanode.DataNodeDriver
import cn.pandadb.driver.values.Node
import cn.pandadb.util.PandaReplyMessage
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.{HippoEndpointRef, HippoRpcEnv, HippoRpcEnvFactory}
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnvClientConfig}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration


trait LeaderNodeTxService {
  def beginTx(): Long;

  def commitTx(txId: Long, isSuccess: Boolean = true): PandaReplyMessage.Value;

  def closeTx(txId: Long): PandaReplyMessage.Value;

  def createNodeInTx(txId: Long,
                     labels: Array[String], properties: Map[String, Any]): Long

//  def deleteNodeInTx(txId: Long, id: Long): Unit
//
//  def addNodeLabelInTx(txId: Long, id: Long, label: String): Unit
//
//  def removeNodeLabelInTx(txId: Long, id: Long, toDeleteLabel: String): Unit
//
//  def setNodePropertyInTx(txId: Long, id: Long, propertiesMap: Map[String, Any]): Unit
//
//  def removeNodePropertyInTx(txId: Long, id: Long, property: String): Unit
}

class LeaderNodeTxServiceImpl(clusterService: ClusterService, config: Config) extends LeaderNodeTxService {
  val dataNodeDriver = new DataNodeDriver
//  val config = new Config()
  val clientConfig = RpcEnvClientConfig(new RpcConf(), "panda-client")

  override def beginTx(): Long = {
    // using System.currentTimeMillis() as txId;
    // todo: using zk generate txId
    var txId = System.currentTimeMillis()
    val (clientRpcEnv, allEndpointRefs) = getDataNodesEndpointRefs(clusterService)

    val succeedCount = allEndpointRefs.par.count(endpointRef => {
      dataNodeDriver.beginTransaction(txId, endpointRef, Duration.Inf) == PandaReplyMessage.SUCCESS
    })
    if (succeedCount < allEndpointRefs.size) {
      allEndpointRefs.par.foreach(endpointRef => {
        dataNodeDriver.closeTransaction(txId, endpointRef, Duration.Inf)
      })
      txId = -1
    }
    clientRpcEnv.shutdown()

    txId
  }

  override def commitTx(txId: Long, isSuccess: Boolean = true): PandaReplyMessage.Value = {
    val (clientRpcEnv, allEndpointRefs) = getDataNodesEndpointRefs(clusterService)
    var res = PandaReplyMessage.SUCCESS

    val succeedCount = allEndpointRefs.par.count(endpointRef => {
      dataNodeDriver.commitTransaction(txId, isSuccess, endpointRef, Duration.Inf) == PandaReplyMessage.SUCCESS
    })
    if (succeedCount < allEndpointRefs.size) {
      res = PandaReplyMessage.FAILED
      allEndpointRefs.par.foreach(endpointRef => {
        dataNodeDriver.commitTransaction(txId, false, endpointRef, Duration.Inf)
      })
    }
    clientRpcEnv.shutdown()

    res
  }

  override def closeTx(txId: Long): PandaReplyMessage.Value = {
    val (clientRpcEnv, allEndpointRefs) = getDataNodesEndpointRefs(clusterService)
    var res = PandaReplyMessage.SUCCESS

    val succeedCount = allEndpointRefs.par.count(endpointRef => {
      dataNodeDriver.closeTransaction(txId, endpointRef, Duration.Inf) == PandaReplyMessage.SUCCESS
    })
    if (succeedCount < allEndpointRefs.size) {
      res = PandaReplyMessage.FAILED
    }
    clientRpcEnv.shutdown()

    res
  }

  override def createNodeInTx(txId: Long, labels: Array[String], properties: Map[String, Any]): Long = {
    null
  }

  private def getDataNodesEndpointRefs(clusterService: ClusterService): (HippoRpcEnv, ArrayBuffer[HippoEndpointRef]) = {
    val dataNodes = clusterService.getDataNodes()
    val clientRpcEnv = HippoRpcEnvFactory.create(clientConfig)
    val allEndpointRefs = ArrayBuffer[HippoEndpointRef]()
    val localAddress = clusterService.getLeaderNode()
    dataNodes.map(s => {
      if (!s.equals(localAddress)) {
        val strs = s.split(":")
        val address = strs(0)
        val port = strs(1).toInt
        val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(address, port), config.getDataNodeEndpointName())
        allEndpointRefs += ref
      }
    })
    (clientRpcEnv, allEndpointRefs)
  }

}
