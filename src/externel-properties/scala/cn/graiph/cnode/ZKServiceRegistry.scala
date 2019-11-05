package cn.graiph.cnode

import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooKeeper}
import org.apache.zookeeper.ZooDefs.Ids

trait ServiceRegistry {
  def registry(serviceName: String, serviceAddress: String);
}

class ZKServiceRegistry extends ServiceRegistry{

  val localhostServiceAddress = ZKConstants.localServiceAddress
  val zkServerAddress = ZKConstants.zkServerAddress

  //error if the watcher is set null
  val zkClient = new ZooKeeper(zkServerAddress,ZKConstants.sessionTimeout,new Watcher {
    override def process(watchedEvent: WatchedEvent): Unit = {
    }
  })

  override def registry(serviceName: String, serviceAddress: String): Unit = {
    val registryPath = ZKConstants.registryPath

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
      zkClient.create(ZKConstants.registryPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
    }

    // Create service node (persistent)
    val servicePath = registryPath + s"/" + serviceName
    if(zkClient.exists(servicePath,false) == null){
      zkClient.create(servicePath,null,Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
    }

    // Create address node (temp)
    val serviceAddress = servicePath +"/" + localhostServiceAddress
    if(zkClient.exists(serviceAddress,false) == null){
      zkClient.create(serviceAddress, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL)
    }

  }

  def registerAsReadNode(serviceAddress: String): Unit ={
    registry(s"read", serviceAddress)
  }

  def registerAsWriteNode(serviceAddress: String): Unit ={
    registry(s"write", serviceAddress)
  }

}

