package cn.pandadb.server

import java.io.{File, FileInputStream}
import java.util.Properties

class ZKConstants(path: String) {

  val prop = new Properties()
  prop.load(new FileInputStream(path))
  val localNodeAddress = prop.getProperty("localNodeAddress")
  val zkServerAddress = prop.getProperty("zkServerAddress")
  val sessionTimeout = prop.getProperty("sessionTimeout").toInt
  val connectionTimeout = prop.getProperty("connectionTimeout")
  val registryPath = prop.getProperty("registryPath")
}
