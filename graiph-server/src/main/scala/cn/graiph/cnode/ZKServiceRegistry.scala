package cn.graiph.cnode

import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooKeeper}
import org.apache.zookeeper.ZooDefs.Ids

trait ServiceRegistry {
  def registry(serviceName: String);
}


class ZKServiceRegistry(zkConstants: ZKConstants) extends ServiceRegistry{

  val localhostServiceAddress = zkConstants.localServiceAddress
  val zkServerAddress = zkConstants.zkServerAddress

  //error if the watcher is set null
  val zkClient = new ZooKeeper(zkServerAddress,zkConstants.sessionTimeout,new Watcher {
    override def process(watchedEvent: WatchedEvent): Unit = {
    }
  })

  override def registry(serviceName: String): Unit = {
    val registryPath = zkConstants.registryPath

    /*    node mode in zk：
    *         gnode
    *        /     \
    *     read    write
    *      /          \
    *  address1     address2
    *
     */
    // Create registry node (persistent)
    if(zkClient.exists(registryPath, false) == null){
      zkClient.create(zkConstants.registryPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
    }

    // Create service node (persistent)
    val servicePath = registryPath + s"/" + serviceName
    if(zkClient.exists(servicePath,false) == null){
      zkClient.create(servicePath,null,Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
    }

    // Create address node (temp)
    val serviceAddress = servicePath +"/" + localhostServiceAddress
    if(zkClient.exists(serviceAddress,false) == null){
      // set CreateMode to EPHEMERAL (don't be EPHEMERAL_SEQUENTIAL)
      zkClient.create(serviceAddress, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
    }

  }

  def registerAsReadNode(serviceAddress: String): Unit ={
    registry(s"read")
  }

  def registerAsWriteNode(serviceAddress: String): Unit ={
    registry(s"write")
  }

}

