package cn.pandadb.server

import java.io.File

object PandaServerEntryPoint {
  def main(args: Array[String]): Unit = {
    val serverBootstrapper = new PandaServerBootstrapper
    val configFile = new File("")
    serverBootstrapper.start(configOverrides = Map("blob.regionfs.zk.address"->"10.0.82.220:2181"))
  }
}
