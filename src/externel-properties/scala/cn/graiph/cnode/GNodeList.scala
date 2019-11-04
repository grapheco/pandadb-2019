package cn.graiph.cnode

import java.io.FileInputStream
import java.util.Properties


/**
  * Created by bluejoe on 2019/11/4.
  */
case class NodeAddress(host: String, port: Int) {

}

object NodeAddress {
  def fromString(url: String, separtor: String = ":" ): NodeAddress = {
    val pair = url.split(separtor)
    NodeAddress(pair(0), pair(1).toInt)
  }
}


object ZKConstants {
  val path = Thread.currentThread().getContextClassLoader.getResource("gNode.properties").getPath;
  val prop = new Properties()
  prop.load(new FileInputStream(path))
  val localServiceAddress = prop.getProperty("localhostServiceAdress")
  val zkServerAddress = prop.getProperty("zkServerAddress")
  val sessionTimeout = prop.getProperty("sessionTimeout").toInt
  val connectionTimeout = prop.getProperty("connectionTimeout")
  val registryPath = prop.getProperty("registryPath")
}

trait GNodeList {
  def getReadNodes(): Array[NodeAddress];

  def getWriteNodes(): Array[NodeAddress];
}

trait GNodeSelector {
  def chooseReadNode(): NodeAddress;

  def chooseWriteNode(): NodeAddress;
}


