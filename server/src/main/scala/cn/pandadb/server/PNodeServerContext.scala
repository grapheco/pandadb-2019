package cn.pandadb.server

import java.io.File

import cn.pandadb.network.ClusterClient
import cn.pandadb.util.ContextMap

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created at 12:49 2019/12/9
  * @Modified By:
  */
object PNodeServerContext extends ContextMap {

  def bindStoreDir(storeDir: File): Unit = {
    GlobalContext.put[File]("pnode.store.dir", storeDir)
  }

  def bindRpcPort(port: Int): Unit = {
    this.put("pnode.rpc.port", port)
  }

  def bindLocalIpAddress(address: String): Unit = {
    this.put("pnode.local.ipAddress", address)
  }

  def bindJsonDataLog(jsonDataLog: JsonDataLog): Unit = {
    this.put[JsonDataLog]("pnode.dataLog", jsonDataLog)
  }

  def bindMasterRole(masterRole: MasterRole): Unit =
    this.put[MasterRole](masterRole)

  def bindClusterClient(client: ClusterClient): Unit =
    this.put[ClusterClient](client)

  def getMasterRole: MasterRole = this.get[MasterRole]

  def getClusterClient: ClusterClient = this.get[ClusterClient]

  def getStoreDir: File = GlobalContext.get[File]("pnode.store.dir")

  def getJsonDataLog: JsonDataLog = this.get[JsonDataLog]("pnode.dataLog")

  def getRpcPort: Int = this.get[Int]("pnode.rpc.port")

  def getLocalIpAddress: String = this.get[String]("pnode.local.ipAddress")

  def bindLeaderNode(boolean: Boolean): Unit =
    this.put("is.leader.node", boolean)

  def isLeaderNode: Boolean = this.getOption("is.leader.node").getOrElse(false)
}
