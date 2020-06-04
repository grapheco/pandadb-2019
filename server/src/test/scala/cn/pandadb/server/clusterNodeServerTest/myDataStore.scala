package cn.pandadb.server.clusterNodeServerTest

import cn.pandadb.store.local.DataStore

class myDataStore extends DataStore(null) {
  var dataVersion: String = null

  override def getDataVersion(): Long = {
    dataVersion.toLong
  }

  override def setDataVersion(version: Long): Unit = {
    dataVersion = version.toString
  }
}
