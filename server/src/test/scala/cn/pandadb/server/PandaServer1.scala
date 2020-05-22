package cn.pandadb.server

import cn.pandadb.configuration.Config

object PandaServer1 {
  def main(args: Array[String]): Unit = {
    val config1 = new Config {
      override def getRpcPort(): Int = 52310

      override def getLocalNeo4jDatabasePath(): String = "output1/db1"
    }
    val pandaServer1 = new PandaServer(config1)
    pandaServer1.start()
  }
}
