package cn.pandadb.server.clusterNodeServerTest

import cn.pandadb.configuration.Config

class myConfig extends Config{

  var nodeAddress: String = null
  var zkAddress: String = null

  override def getNodeAddress(): String = {
    nodeAddress
  }

  override def getZKAddress(): String = {
    zkAddress
  }
}
