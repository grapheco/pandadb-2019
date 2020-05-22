package cn.pandadb.server

import cn.pandadb.configuration.Config

object PandaServer2 {
  def main(args: Array[String]): Unit = {
    val config2 = new Config {
      override def getRpcPort(): Int = 52320

      override def getLocalNeo4jDatabasePath(): String = "output2/db2"
    }
    val pandaServer2 = new PandaServer(config2)
    pandaServer2.start()
  }
}
