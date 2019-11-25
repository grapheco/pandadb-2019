package cn.pandadb.server

import cn.pandadb.network.{ClusterEventListener, ClusterState, NodeAddress, ZookeeperBasedClusterManager}


/**
  * Created by bluejoe on 2019/11/14.
  */
trait CoordinatorServer {
  def stop();
}
