package cn.graiph.cnode

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
  });

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
    val gNodelist = nodeList.toArray
    //at least 1 read node
    if(gNodelist.length < 1){
      throw new Exception(s"Readable node is less than 1.")
    }
    gNodelist
  }

  override def getWriteNodes(): Array[NodeAddress] = {
    val children = zkClient.getChildren(writeNodePath,true)
    val nodeList = ArrayBuffer[NodeAddress]()

    for(child <- children){
      nodeList.append(NodeAddress.fromString(child))
    }
    val gNodelist = nodeList.toArray
    //at least 2 write nodes
    if(gNodelist.length < 2){
      throw new Exception(s"Writable nodes are less than 2.")
    }
    gNodelist
  }

}

//TO DO: implement fully functioned selector
class ZKGNodeSelector extends GNodeSelector{
  val zkGNodeList = new ZKGNodeList;

  override def chooseReadNode(): NodeAddress = {
    val readNodeList = zkGNodeList.getReadNodes()
    readNodeList(0)
  }

  override def chooseWriteNode(): NodeAddress = {
    val writeNodeList = zkGNodeList.getWriteNodes()
    writeNodeList(1)
  }

}