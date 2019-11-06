package cn.graiph.cnode
import java.io.{File, FileInputStream}
import java.util.Properties

import cn.graiph.server.GraiphServer

trait Launcher {


}


class GNodeLauncher(dbPath: String, confPath:String) extends Launcher {

  val serviceAddress = {
    val prop = new Properties()
    prop.load(new FileInputStream(confPath))
    prop.getProperty("serviceAddress")
  }

  def startServer(): Unit ={
    GraiphServer.startServer(new File(dbPath), new File(confPath))
  }

  def registerAsReadNode(): Unit = {
    new ZKServiceRegistry().registry("read", serviceAddress)
  }

  def registerAsWriteNode(): Unit = {
    new ZKServiceRegistry().registry("write", serviceAddress)
  }

}

class CoordinatorLauncher(dbPath: String, confPath: String){

  def startServer(dbPath: String, confPath: String): Unit ={
    GraiphServer.startServer(new File(dbPath), new File(confPath))
  }
}
