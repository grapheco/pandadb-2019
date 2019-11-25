package cn.pandadb.server

import cn.pandadb.network.ZKConstants
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

  def registry(servicePath: String): Unit = {
    val registryPath = zkConstants.registryPath
    val serviceAddress = servicePath + s"/" + localNodeAddress
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
    curator.create()
      .creatingParentsIfNeeded()
      .withMode(CreateMode.PERSISTENT)
      .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
      .forPath(registryPath)


    // Create service node (persistent)
    curator.create()
      .creatingParentsIfNeeded()
      .withMode(CreateMode.PERSISTENT)
      .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
      .forPath(servicePath)

    // Create address node (temp)
    curator.create()
      .creatingParentsIfNeeded()
      .withMode(CreateMode.EPHEMERAL)
      .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
      .forPath(serviceAddress)
  }

  def registerAsOrdinaryNode(serviceAddress: String): Unit = {
    registry(zkConstants.ordinaryNodesPath)
  }

  def registerAsLeader(serviceAddress: String): Unit = {
    registry(zkConstants.leaderNodePath)
  }

}
