package cn.pandadb.server

import java.io.File

object PandaServerEntryPoint {
  def main(args: Array[String]): Unit = {
    val serverBootstrapper = new PandaServerBootstrapper
    val configFile = new File("")
    serverBootstrapper.start()
  }
}
