package cn.pandadb.server

import java.io.File

import cn.pandadb.externalprops.ExternalPropertyStoreFactory
import cn.pandadb.network.ClusterClient
import cn.pandadb.util.InstanceContext

/**
  * wrapper of InstanceContext
  */
object PNodeServerContext {

  def bindStoreDir(storeDir: File): Unit = {
    InstanceContext.put[File]("pnode.store.dir", storeDir)
  }

  def bindRpcPort(port: Int): Unit = {
    InstanceContext.put("pnode.rpc.port", port)
  }

  def bindLocalIpAddress(address: String): Unit = {
    InstanceContext.put("pnode.local.ipAddress", address)
  }

  def bindJsonDataLog(jsonDataLog: JsonDataLog): Unit = {
    InstanceContext.put[JsonDataLog]("pnode.dataLog", jsonDataLog)
  }

  def bindMasterRole(masterRole: MasterRole): Unit =
    InstanceContext.put[MasterRole](masterRole)

  def bindClusterClient(client: ClusterClient): Unit =
    InstanceContext.put[ClusterClient](client)

  def bindLeaderNode(boolean: Boolean): Unit =
    InstanceContext.put("is.leader.node", boolean)

  def bindExternalPropStorage(boolean: Boolean): Unit =
    InstanceContext.put("external.property.storage.enabled", boolean)

  def bindExternalPropStorageFactory(exPropStorageFactory: ExternalPropertyStoreFactory): Unit =
    InstanceContext.put[ExternalPropertyStoreFactory](exPropStorageFactory)

  def getMasterRole: MasterRole = InstanceContext.get[MasterRole]

  def getClusterClient: ClusterClient = InstanceContext.get[ClusterClient]

  def getStoreDir: File = InstanceContext.get[File]("pnode.store.dir")

  def getJsonDataLog: JsonDataLog = InstanceContext.get[JsonDataLog]("pnode.dataLog")

  def getRpcPort: Int = InstanceContext.get[Int]("pnode.rpc.port")

  def getLocalIpAddress: String = InstanceContext.get[String]("pnode.local.ipAddress")

  def getExternalPropStorageFactory: ExternalPropertyStoreFactory = InstanceContext.get[ExternalPropertyStoreFactory]

  def isLeaderNode: Boolean = InstanceContext.getOption("is.leader.node").getOrElse(false)

  def isExternalPropStorageEnabled: Boolean = InstanceContext.getOption("external.property.storage.enabled").getOrElse(false)

}
