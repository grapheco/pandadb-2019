package cn.pandadb.server

import cn.pandadb.network.{ZKConstants, ZKPathConfig}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.{CreateMode, ZooDefs}


trait ServiceRegistry {

  //service name: ordinaryNode, leader
  def registry(serviceName: String)
  // TODO: new join node func implement
  //  def getLog()
  //  def getVersion()

}

class ZKServiceRegistry(zkConstants: ZKConstants) extends ServiceRegistry {

  val localNodeAddress = zkConstants.localNodeAddress
  val zkServerAddress = zkConstants.zkServerAddress
  val curator: CuratorFramework = CuratorFrameworkFactory.newClient(zkConstants.zkServerAddress,
    new ExponentialBackoffRetry(1000, 3));
  curator.start()

  def registry(servicePath: String): Unit = {
    val registryPath = ZKPathConfig.registryPath
    val nodeAddress = servicePath + s"/" + localNodeAddress
/*    node mode in zk：
    *                     pandaDB
    *               /        |        \
    *           /            |           \
    *   ordinaryNodes     leader          data
    *      /                 |              \
    *  addresses        leaderAddress     version(not implemented)
    *
    */

    // Create registry node (pandanode, persistent)
    if(curator.checkExists().forPath(registryPath) == null) {
      curator.create()
        .creatingParentsIfNeeded()
        .withMode(CreateMode.PERSISTENT)
        .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
        .forPath(registryPath)
    }

    // Create service node (persistent)
    if(curator.checkExists().forPath(servicePath) == null) {
      curator.create()
        .creatingParentsIfNeeded()
        .withMode(CreateMode.PERSISTENT)
        .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
        .forPath(servicePath)
    }

    // Create address node (temp)
    if(curator.checkExists().forPath(nodeAddress) == null) {
      curator.create()
        .creatingParentsIfNeeded()
        .withMode(CreateMode.EPHEMERAL)
        .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
        .forPath(nodeAddress)
    }

  }

  def registerAsOrdinaryNode(serviceAddress: String): Unit = {
    registry(ZKPathConfig.ordinaryNodesPath)
  }

  def registerAsLeader(serviceAddress: String): Unit = {
    registry(ZKPathConfig.leaderNodePath)
  }

  // ugly funcion! to satisfy curator event listener.
  def unRegister(serviceAddress: String): Unit = {
    val ordinaryNodePath = ZKPathConfig.ordinaryNodesPath + s"/" + serviceAddress
    val leaderNodePath = ZKPathConfig.leaderNodePath + s"/" + serviceAddress
    if(curator.checkExists().forPath(ordinaryNodePath) != null) {
      curator.delete().forPath(ordinaryNodePath)
    }
    if(curator.checkExists().forPath(leaderNodePath) != null) {
      curator.delete().forPath(leaderNodePath)
    }
  }


}
