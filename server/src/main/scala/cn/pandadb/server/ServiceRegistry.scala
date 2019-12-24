package cn.pandadb.server

import cn.pandadb.network.{NodeAddress, ZKPathConfig}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.{CreateMode, ZooDefs}

trait ServiceRegistry {

  def registry(servicePath: String, localNodeAddress: String)
}

class ZKServiceRegistry(zkString: String) extends ServiceRegistry {

  var localNodeAddress: String = _
  val zkServerAddress = zkString
  val curator: CuratorFramework = CuratorFrameworkFactory.newClient(zkServerAddress,
    new ExponentialBackoffRetry(1000, 3));
  curator.start()

  def registry(servicePath: String, localNodeAddress: String): Unit = {
    val registryPath = ZKPathConfig.registryPath
    val nodeAddress = servicePath + s"/" + localNodeAddress
    /*
    * node mode in zk：
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

  def registerAsOrdinaryNode(nodeAddress: NodeAddress): Unit = {
    localNodeAddress = nodeAddress.getAsString
    registry(ZKPathConfig.ordinaryNodesPath, localNodeAddress)
  }

  def registerAsLeader(nodeAddress: NodeAddress): Unit = {
    localNodeAddress = nodeAddress.getAsString
    registry(ZKPathConfig.leaderNodePath, localNodeAddress)
  }

  def unRegisterOrdinaryNode(node: NodeAddress): Unit = {
    val nodeAddress = node.getAsString
    val ordinaryNodePath = ZKPathConfig.ordinaryNodesPath + s"/" + nodeAddress
    if(curator.checkExists().forPath(ordinaryNodePath) != null) {
      curator.delete().forPath(ordinaryNodePath)
    }
  }

  def unRegisterLeaderNode(node: NodeAddress): Unit = {
    val nodeAddress = node.getAsString
    val leaderNodePath = ZKPathConfig.leaderNodePath + s"/" + nodeAddress
    if(curator.checkExists().forPath(leaderNodePath) != null) {
      curator.delete().forPath(leaderNodePath)
    }
  }
}
