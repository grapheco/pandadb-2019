package cn.pandadb.tool

import java.io.File

import cn.pandadb.server.PNodeServer

/**
  * Created by bluejoe on 2019/7/17.
  */
object PNodeServerStarter {
  def main(args: Array[String]) {
    if (args.length != 2) {
      sys.error(s"Usage:\r\n");
      sys.error(s"GNodeServerStarter <db-dir> <conf-file>\r\n");
    }

    PNodeServer.startServer(new File(args(0)),
      new File(args(1)));
  }
}