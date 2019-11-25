package cn.pandadb.network

import cn.pandadb.util.{ ConfigurationEx}

class ZKConstants(conf: ConfigurationEx) {

  val localNodeAddress = conf.getRequiredValueAsString(s"localNodeAddress")
  val zkServerAddress = conf.getRequiredValueAsString(s"zkServerAddress")
  val sessionTimeout = conf.getRequiredValueAsInt(s"sessionTimeout")
  val connectionTimeout = conf.getRequiredValueAsInt(s"connectionTimeout")
  val registryPath = conf.getRequiredValueAsString(s"registryPath")
  val ordinaryNodesPath = conf.getRequiredValueAsString(s"ordinaryNodesPath")
  val leaderNodePath = conf.getRequiredValueAsString(s"leaderNodePath")
}
