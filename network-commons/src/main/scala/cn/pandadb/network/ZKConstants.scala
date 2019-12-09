package cn.pandadb.network

import java.io.{File, FileInputStream}
import java.util.Properties

import cn.pandadb.util.ConfigurationEx

class ZKConstants(conf: ConfigurationEx) {

  val localNodeAddress = conf.getRequiredValueAsString(s"localNodeAddress")
  // construct
  val zkServerAddress = conf.getRequiredValueAsString(s"zkServerAddress")
}
