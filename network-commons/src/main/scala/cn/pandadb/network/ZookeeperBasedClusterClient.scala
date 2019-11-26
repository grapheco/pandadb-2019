package cn.pandadb.network

import java.util

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created in 9:06 2019/11/26
  * @Modified By:
  */

// zk agent client  no listener
class ZookeerperBasedClusterClient(zkString: String) extends ClusterClient {

  val curator: CuratorFramework = CuratorFrameworkFactory.newClient(zkString,
    new ExponentialBackoffRetry(1000, 3));
  curator.start()

  // avoid outter write
  private var currentState: ClusterState = _

  val registryPath = ZKPathConfig.registryPath
  val leaderPath = ZKPathConfig.leaderNodePath
  val ordinaryPath = ZKPathConfig.ordinaryNodesPath

  var listenerList: List[ZKClusterEventListener] = List[ZKClusterEventListener]()

  // query from zk when init.
  var availableNodes: Set[NodeAddress] = {
    val arrayList = curator.getChildren.forPath(ordinaryPath).toArray
    for (nodeAddressStr <- arrayList) {
      availableNodes += NodeAddress.fromString(nodeAddressStr.toString)
    }
    availableNodes
  }

  // Is this supposed to be right? listenerList is null at this time.
  //new ZKServiceDiscovery(curator, zkConstants, listenerList)

  override def getWriteMasterNode(): NodeAddress = {
    val leaderAddress = curator.getChildren().forPath(leaderPath).toString
    NodeAddress.fromString(leaderAddress)
  }

  // return variable availableNodes, don't query from zk every time.
  override def getAllNodes(): Iterable[NodeAddress] = {
    availableNodes
  }


  override def getCurrentState(): ClusterState = {
    currentState
  }

  // no use at this period.
  override def listen(listener: ClusterEventListener): Unit = {
    listenerList = listener.asInstanceOf[ZKClusterEventListener] :: listenerList
  }

  override def waitFor(state: ClusterState): Unit = null

}
