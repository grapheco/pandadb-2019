package cn.pandadb.server.clusterNodeServerTest

import cn.pandadb.server.Store.DataStore

class myDataStore extends DataStore(null) {
  var dataVersion: String = null

  override def getDataVersion(): Long = {
    dataVersion.toLong
  }

  override def setDataVersion(version: Long): Unit = {
    dataVersion = version.toString
  }
}
