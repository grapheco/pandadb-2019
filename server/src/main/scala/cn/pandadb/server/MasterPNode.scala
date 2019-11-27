package cn.pandadb.server

import cn.pandadb.network.{NodeAddress, ZKClusterEventListener}
import org.apache.curator.framework.CuratorFramework

/**
  * @Author: Airzihao
  * @Description: This class is instanced when a node is selected as master node.
  * @Date: Created at 13:13 2019/11/27
  * @Modified By:
  */

trait Master {

  // suppose to get from real transaction.
  val cypher: String
  val allNodes: Iterable[NodeAddress]

  // it should be a distributed cluster agent, not only curator.
  val curator: CuratorFramework

  // delay all write/read requests, implements by curator
  def globalWriteLock()

  // delay write requests only, implements by curator
  def globalReadLock()

  // inform these listeners the cluster context change?
  var listenerList: List[ZKClusterEventListener]

}

//
class MasterPNode {

}
