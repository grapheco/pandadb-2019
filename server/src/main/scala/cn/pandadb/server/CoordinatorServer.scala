package cn.pandadb.server

import cn.pandadb.network.{ClusterEventListener, ClusterState, NodeAddress, ZookeeperBasedClusterManager}


/**
  * Created by bluejoe on 2019/11/14.
  */
trait CoordinatorServer {
  def stop();
}

class ZKClusterManager(zkString: String, zkConstants: ZKConstants) extends ZookeeperBasedClusterManager(zkString: String) {


  override def getWriteMasterNode(): NodeAddress = ???

  override def getCurrentState(): ClusterState = ???

  override def listen(listener: ClusterEventListener): Unit = ???

  override def getAllNodes(): Iterable[NodeAddress] = ???

  override def waitFor(state: ClusterState): Unit = ???


}