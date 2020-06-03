package cn.pandadb.server

import cn.pandadb.configuration.Config

object PandaServer2 {
  def main(args: Array[String]): Unit = {
    val config2 = new Config()
    config2.withSettings(Map("rpc.listen.port"->"52320",
      "blob.regionfs.zk.address"->"10.0.82.220:2181",
      "db.local.data.path" -> "testdata/data2"
    ))

    val pandaServer2 = new PandaServer(config2)
    pandaServer2.start()
  }
}
