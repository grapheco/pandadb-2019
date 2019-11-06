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

  var cachedReadNodeSet: Set[NodeAddress] = Set();
  var cachedWriteNodeSet: Set[NodeAddress] = Set();

  val readNodePath = ZKConstants.registryPath + "/" + "read"
  val writeNodePath = ZKConstants.registryPath + "/" + "write"

  var listenerList: List[GNodeListListener] = List[GNodeListListener]();

  def updataServers() {

    val updatedReadNodeSet = getReadNodes()
    val updatedWriteNodeSet = getWriteNodes()

    // handle online read nodes
    val onlineReadNodes = getOnlineNode(cachedReadNodeSet, updatedReadNodeSet)
    for(addr <- onlineReadNodes) {
      for(listener <- listenerList) {
        listener.onEvent(ReadGNodeConnected(addr))
      }
    }

    // handle offline read nodes
    val offlineReadNodes = getOfflineNode(cachedReadNodeSet, updatedReadNodeSet)
    for(addr <- offlineReadNodes) {
      for(listener <- listenerList) {
        listener.onEvent(ReadGNodeDisconnected(addr))
      }
    }

    // handle online write nodes
    val onlineWriteNodes = getOnlineNode(cachedWriteNodeSet, updatedWriteNodeSet)
    for(addr <- onlineWriteNodes) {
      for(listener <- listenerList) {
        listener.onEvent(WriteGNodeConnected(addr))
      }
    }

    // handle offline write nodes
    val offlineWriteNodes = getOfflineNode(cachedWriteNodeSet, updatedWriteNodeSet)
    for(addr <- offlineWriteNodes) {
      for(listener <- listenerList) {
        listener.onEvent(WriteGNodeDisconnected(addr))
      }
    }

    // update the cached Nodes
    cachedReadNodeSet = updatedReadNodeSet
    cachedWriteNodeSet = updatedWriteNodeSet
  }


  def getOnlineNode(cachedSet: Set[NodeAddress], updatedSet: Set[NodeAddress]): Set[NodeAddress] = {
    val intersectSet = cachedSet.intersect(updatedSet)
    updatedSet -- intersectSet
  }

  def getOfflineNode(cachedSet: Set[NodeAddress], updatedSet: Set[NodeAddress]): Set[NodeAddress] = {
    val intersectSet = cachedSet.intersect(updatedSet)
    cachedSet -- intersectSet
  }


  override def addListener(listener: GNodeListListener): Unit = {
    listenerList = listener :: listenerList
  }

  def getReadNodes(): Set[NodeAddress] = {
    val children = zkClient.getChildren(readNodePath,true)
    val nodeList = ArrayBuffer[NodeAddress]()
    for(child <- children){
      nodeList.append(NodeAddress.fromString(child))
    }
    val gNodelist = nodeList.toArray
    //at least 1 read node
    if(gNodelist.length < 1){
      throw new Exception(s"Available read node is less than 1.")
    }
    gNodelist.toSet
  }

  def getWriteNodes(): Set[NodeAddress] = {
    val children = zkClient.getChildren(writeNodePath,true)
    val nodeList = ArrayBuffer[NodeAddress]()

    for(child <- children){
      nodeList.append(NodeAddress.fromString(child))
    }
    val gNodelist = nodeList.toArray
    //at least 2 write nodes
    if(gNodelist.length < 2){
      throw new Exception(s"Available write nodes are less than 2.")
    }
    gNodelist.toSet
  }

}