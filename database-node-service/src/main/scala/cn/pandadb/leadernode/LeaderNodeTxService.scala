package cn.pandadb.leadernode

import cn.pandadb.cluster.ClusterService
import cn.pandadb.configuration.Config
import cn.pandadb.datanode.DataNodeDriver
import cn.pandadb.driver.values.Node
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.RpcEnvClientConfig


trait LeaderNodeTxService {
  def beginTx(): Long;

  def commitTx(txId: Long): Unit;

  def closeTx(txId: Long): Unit;

  def createNodeInTx(txId: Long,
                     id: Long, labels: Array[String], properties: Map[String, Any]): Node

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
    System.currentTimeMillis()
  }

  override def commitTx(txId: Long): Unit = {}

  override def closeTx(txId: Long): Unit = {}

  override def createNodeInTx(txId: Long, id: Long, labels: Array[String], properties: Map[String, Any]): Node = {
    null
  }
//
//  override def deleteNodeInTx(txId: Long, id: Long): Unit = ???
//
//  override def addNodeLabelInTx(txId: Long, id: Long, label: String): Unit = ???
//
//  override def removeNodeLabelInTx(txId: Long, id: Long, toDeleteLabel: String): Unit = ???
//
//  override def setNodePropertyInTx(txId: Long, id: Long, propertiesMap: Map[String, Any]): Unit = ???
//
//  override def removeNodePropertyInTx(txId: Long, id: Long, property: String): Unit = ???

}
