package cn.pandadb.server

import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooKeeper}

trait ServiceRegistry {

  //service name: ordinaryNode, leader
  def registry(serviceName: String)
}

class ZKServiceRegistry(zkConstants: ZKConstants) extends ServiceRegistry {

  val localNodeAddress = zkConstants.localNodeAddress
  val zkServerAddress = zkConstants.zkServerAddress

  val zkClient = new ZooKeeper(zkServerAddress, zkConstants.sessionTimeout, new Watcher {
    override def process(event: WatchedEvent): Unit = {

    }
  })

  override def registry(serviceName: String): Unit = {
    val registryPath = zkConstants.registryPath
/*    node mode in zk：
    *           gnode
    *        /         \
    *   ordinaryNodes  leader
    *      /              \
    *  addresses      leaderAddress
    *
    */

    // Create registry node (pandanode, persistent)
    if(zkClient.exists(registryPath, false) == null) {
      zkClient.create(zkConstants.registryPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
    }

    // Create service node (persistent)
    val servicePath = registryPath + s"/" + serviceName
    if(zkClient.exists(servicePath, false) == null) {
      zkClient.create(servicePath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
    }

    // Create address node (temp)
    val serviceAddress = servicePath + s"/" + localNodeAddress
    if(zkClient.exists(serviceAddress, false) == null) {
      zkClient.create(serviceAddress, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
    }

  }

  def registerAsOrdinaryNode(serviceAddress: String): Unit = {
    registry(s"ordinaryNode")
  }

  def registerAsLeader(serviceAddress: String): Unit = {
    registry(s"leaderNode")
  }



}
