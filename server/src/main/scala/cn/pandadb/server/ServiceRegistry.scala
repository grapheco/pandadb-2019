package cn.pandadb.server

import cn.pandadb.network.{NodeAddress, ZKConstants, ZKPathConfig}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.{CreateMode, ZooDefs}


trait ServiceRegistry {

  //service name: ordinaryNode, leader
  def registry(servicePath: String, localNodeAddress: String)
  // TODO: new join node func implement
  //  def getLog()
  //  def getVersion()

}

// TODO: Reconstruct this class, localNodeAddress as a parameter.
class ZKServiceRegistry(zkString: String) extends ServiceRegistry {

  var localNodeAddress: String = _
  val zkServerAddress = zkString
  val curator: CuratorFramework = CuratorFrameworkFactory.newClient(zkServerAddress,
    new ExponentialBackoffRetry(1000, 3));
  curator.start()



  def registry(servicePath: String, localNodeAddress: String): Unit = {
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

  def registerAsOrdinaryNode(nodeAddress: String): Unit = {
    localNodeAddress = nodeAddress
    registry(ZKPathConfig.ordinaryNodesPath, localNodeAddress)
  }

  def registerAsOrdinaryNode(nodeAddress: NodeAddress): Unit = {
    localNodeAddress = nodeAddress.getAsStr()
    registry(ZKPathConfig.ordinaryNodesPath, localNodeAddress)
  }


  def registerAsLeader(nodeAddress: NodeAddress): Unit = {
    localNodeAddress = nodeAddress.getAsStr()
    registry(ZKPathConfig.leaderNodePath, localNodeAddress)
  }

  def registerAsLeader(nodeAddress: String): Unit = {
    localNodeAddress = nodeAddress
    registry(ZKPathConfig.leaderNodePath, localNodeAddress)
  }

  // ugly funcion! to satisfy curator event listener.
  def unRegisterOrdinaryNode(node: String): Unit = {
    val nodeAddress = node
    val ordinaryNodePath = ZKPathConfig.ordinaryNodesPath + s"/" + nodeAddress
    if(curator.checkExists().forPath(ordinaryNodePath) != null) {
      curator.delete().forPath(ordinaryNodePath)
    }
  }

  def unRegisterOrdinaryNode(node: NodeAddress): Unit = {
    val nodeAddress = node.getAsStr()
    val ordinaryNodePath = ZKPathConfig.ordinaryNodesPath + s"/" + nodeAddress
    if(curator.checkExists().forPath(ordinaryNodePath) != null) {
      curator.delete().forPath(ordinaryNodePath)
    }
  }

  def unRegisterLeaderNode(node: String): Unit = {
    val nodeAddress = node
    val leaderNodePath = ZKPathConfig.leaderNodePath + s"/" + nodeAddress
    if(curator.checkExists().forPath(leaderNodePath) != null) {
      curator.delete().forPath(leaderNodePath)
    }
  }

  def unRegisterLeaderNode(node: NodeAddress): Unit = {
    val nodeAddress = node.getAsStr()
    val leaderNodePath = ZKPathConfig.leaderNodePath + s"/" + nodeAddress
    if(curator.checkExists().forPath(leaderNodePath) != null) {
      curator.delete().forPath(leaderNodePath)
    }
  }

}
