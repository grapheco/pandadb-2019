package cn.pandadb.server

import cn.pandadb.configuration.Config

object PandaServer1 {
  def main(args: Array[String]): Unit = {
    val config1 = new Config()
    config1.withSettings(Map("rpc.listen.port"->"52310",
                              "blob.regionfs.zk.address"->"10.0.82.220:2181",
                              "db.local.data.path" -> "testdata/data1"
    ))
    val pandaServer1 = new PandaServer(config1)
    pandaServer1.start()
  }
}
