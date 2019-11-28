package cn.pandadb.network

import java.io.{File, FileInputStream}
import java.util.Properties

import cn.pandadb.util.ConfigurationEx

// some config should be fixed
class ZKConstants(conf: ConfigurationEx) {

  val localNodeAddress = conf.getRequiredValueAsString(s"localNodeAddress")
  // construct
  val zkServerAddress = conf.getRequiredValueAsString(s"zkServerAddress")
//  val sessionTimeout = conf.getRequiredValueAsInt(s"sessionTimeout")
//  val connectionTimeout = conf.getRequiredValueAsInt(s"connectionTimeout")
}
