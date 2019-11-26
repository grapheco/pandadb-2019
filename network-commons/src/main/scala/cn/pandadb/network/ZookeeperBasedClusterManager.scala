package cn.pandadb.network

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created in 9:06 2019/11/26
  * @Modified By:
  */

class ZookeerperBasedClusterManager(zkConstants: ZKConstants) extends ClusterClient with ClusterEventListener {

  val curator: CuratorFramework = CuratorFrameworkFactory.newClient(zkConstants.zkServerAddress, new ExponentialBackoffRetry(1000, 3));
  curator.start()
  private var currentState: ClusterState = _
  val registryPath = zkConstants.registryPath
  val leaderPath = zkConstants.leaderNodePath
  val ordinaryPath = zkConstants.ordinaryNodesPath

  var listenerList: List[ZKClusterEventListener] = List[ZKClusterEventListener]()

  var availableNodes: Set[NodeAddress] = _;

  // Is this supposed to be right? listenerList is null at this time.
  new ZKServiceDiscovery(curator, zkConstants, listenerList)

  override def getWriteMasterNode(): NodeAddress = {
    val leaderAddress = curator.getChildren().forPath(leaderPath).toString
    NodeAddress.fromString(leaderAddress)
  }

  // return variable availableNodes, don't query from zk every time.
  override def getAllNodes(): Iterable[NodeAddress] = {
    val ordinaryNodes = curator.getChildren.forPath(ordinaryPath).iterator()
    var nodeAddresses: List[NodeAddress] = Nil
    while (ordinaryNodes.hasNext) {
      nodeAddresses = nodeAddresses :+ NodeAddress.fromString(ordinaryNodes.next())
    }
    val allNodes = nodeAddresses;
    allNodes
  }


  override def getCurrentState(): ClusterState = {
    currentState
  }

  // is this the func to add listener?
  override def listen(listener: ClusterEventListener): Unit = {
    listenerList = listener.asInstanceOf[ZKClusterEventListener] :: listenerList
  }

  override def waitFor(state: ClusterState): Unit = null


  override def onEvent(event: ClusterEvent): Unit = {
    event match {
      // Not implemented.
      case ClusterStateChanged() => ;

      // update availableNodes in ZKBasedClusterManager
      case NodeConnected(nodeAddress) =>
        availableNodes = availableNodes + nodeAddress;
      case NodeDisconnected(nodeAddress) =>
        availableNodes = availableNodes - nodeAddress;

      case ReadRequestAccepted() => ;
      case WriteRequestAccepted() => ;
      case ReadRequestCompleted() => ;
      case WriteRequestCompleted() => ;
      case MasterWriteNodeSeleted() => ;
      case READY_TO_WRITE() => ;
      case WRITE_FINISHED() => ;

    }
  }

}
