package cn.graiph.cnode

import java.io.FileInputStream
import java.util.Properties

import org.neo4j.driver.Driver


/**
  * Created by bluejoe on 2019/11/4.
  */
case class NodeAddress(host: String, port: Int) {
  def getUrl(): String ={
    return host + s":" + port.toString
  }
}

object NodeAddress {
  def fromString(url: String, seperator: String = ":"): NodeAddress = {
    val pair = url.split(seperator)
    NodeAddress(pair(0), pair(1).toInt)
  }
}

class ZKConstants(path: String) {
  //val path = Thread.currentThread().getContextClassLoader.getResource("gNode.properties").getPath;
  //val path = s"./testdata/gnode1.conf"

  val prop = new Properties()
  prop.load(new FileInputStream(path))
  val localServiceAddress = prop.getProperty("serviceAddress")
  val zkServerAddress = prop.getProperty("zkServerAddress")
  val sessionTimeout = prop.getProperty("sessionTimeout").toInt
  val connectionTimeout = prop.getProperty("connectionTimeout")
  val registryPath = prop.getProperty("registryPath")
}

// implemented
trait GNodeListListener {
  def onEvent(event: GNodeListEvent);
}

// implemented
trait GNodeListEvent {

}

case class ReadGNodeConnected(address: NodeAddress) extends GNodeListEvent {

}

case class ReadGNodeDisconnected(address: NodeAddress) extends GNodeListEvent {

}

case class WriteGNodeConnected(address: NodeAddress) extends GNodeListEvent {

}

case class WriteGNodeDisconnected(address: NodeAddress) extends GNodeListEvent {

}

trait GNodeList {
  def addListener(listener: GNodeListListener);
}

trait GNodeSelector {
  def chooseReadNode(): Driver;

  def chooseWriteNode(): Driver;
}


