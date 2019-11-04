package cn.graiph.cnode

import java.io.IOException
import java.lang.Exception
import java.util

import scala.collection.JavaConversions._
import org.apache.zookeeper.Watcher.Event.EventType
import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper}

import scala.collection.mutable.ArrayBuffer


class ZKGNodeList extends GNodeList {
  val zkServerAddress = ZKConstants.zkServerAddress

  val zkClient = new ZooKeeper(zkServerAddress, ZKConstants.sessionTimeout, new Watcher(){

    override def process(watchedEvent: WatchedEvent): Unit = {
      if(watchedEvent.getType == EventType.None)
        return
      try {
        updataServers();
      } catch {
        case ex: Exception =>{
          ex.printStackTrace()
        }
      }
    }
  })

  val readNodePath = ZKConstants.registryPath + "/" + "read"
  val writeNodePath = ZKConstants.registryPath + "/" + "write"

  //TO DO: How to implement?
  def updataServers() {
      val childrenList = zkClient.getChildren(ZKConstants.registryPath,true)

  }


  override def getReadNodes(): Array[NodeAddress] = {
    val children = zkClient.getChildren(readNodePath,true)
    val nodeList = ArrayBuffer[NodeAddress]()
    for(child <- children){
      nodeList.append(NodeAddress.fromString(child))
    }
    nodeList.toArray
  }

  override def getWriteNodes(): Array[NodeAddress] = {
    val children = zkClient.getChildren(writeNodePath,true)
    val nodeList = ArrayBuffer[NodeAddress]()
    for(child <- children){
      nodeList.append(NodeAddress.fromString(child))
    }
    nodeList.toArray
  }

}
