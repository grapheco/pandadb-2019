package cn.graiph.cnode
import java.io.{File, FileInputStream}
import java.util.Properties

import cn.graiph.server.GraiphServer
import org.neo4j.bolt.v1.runtime.BoltAuthenticationHelper

trait Launcher {

}

class GNodeLauncher(dbPath: String, confPath:String) extends Launcher {

  val zkConstants: ZKConstants = new ZKConstants(confPath)
  val serviceAddress = {
    val prop = new Properties()
    prop.load(new FileInputStream(confPath))
    prop.getProperty("serviceAddress")
  }

  def startServer(): Unit ={
    GraiphServer.startServer(new File(dbPath), new File(confPath))
  }

  def registerAsReadNode(): Unit = {
    new ZKServiceRegistry(zkConstants).registry("read")
  }

  def registerAsWriteNode(): Unit = {
    new ZKServiceRegistry(zkConstants).registry("write")
  }

}


class CNodeLauncher(dbPath: String, confPath: String){

  val zkConstants: ZKConstants = new ZKConstants(confPath)
  val zkNodeList = new ZKGNodeList(new ZKConstants(confPath))

  def startServer(): Unit ={
    GraiphServer.startServer(new File(dbPath), new File(confPath))
    zkNodeList.addListener(new PooledGNodeSelector)
  }

}
