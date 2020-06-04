package cn.pandadb.server

import java.io.File

object PandaServerEntryPoint {

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      sys.error(s"Usage:\r\n");
      sys.error(s"PandaServerEntryPoint <conf-file>\r\n");
    }

    val serverBootstrapper = new PandaServerBootstrapper
    val configFile = new File(args(0))
    if (configFile.exists() && configFile.isFile()) {
      serverBootstrapper.start(Some(configFile))
    }
    else {
      sys.error("can not find <conf-file> \r\n")
    }
//    serverBootstrapper.start(configOverrides = Map("blob.regionfs.zk.address"->"10.0.82.220:2181"))
  }


}
