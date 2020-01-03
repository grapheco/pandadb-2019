package cn.pandadb.tool

import java.io.File

import cn.pandadb.server.PNodeServer
import cn.pandadb.util.GlobalContext

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

object WatchDogStarter {
  def main(args: Array[String]) {
    if (args.length != 2) {
      sys.error(s"Usage:\r\n");
      sys.error(s"WatchDogStarter <db-dir> <conf-file>\r\n");
    }

    GlobalContext.setWatchDog(true);
    //FIXME: redundant storedir
    PNodeServer.startServer(new File(args(0)),
      new File(args(1)));
  }
}