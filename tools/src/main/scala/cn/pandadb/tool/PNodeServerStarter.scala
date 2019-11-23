package cn.pandadb.tool

import java.io.File

import cn.pandadb.server.PNodeServer

/**
  * Created by bluejoe on 2019/7/17.
  */
object PNodeServerStarter {
  def main(args: Array[String]) {
    //TODO: check args
    PNodeServer.startServer(new File(args(0)),
      new File(args(1)));
  }
}
