package cn.graiph.cnode

import java.io.FileInputStream
import java.net.InetAddress
import java.util.Properties

import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooKeeper}
import org.apache.zookeeper.ZooDefs.Ids

trait ServiceRegistry {
  def registry(serviceName: String, serviceAddress: String);
}


class ZKServiceRegistry extends ServiceRegistry{

  val localhostServiceAddress = ZKConstants.localServiceAddress
  val zkServerAddress = ZKConstants.zkServerAddress

  val zkClient = new ZooKeeper(zkServerAddress,ZKConstants.sessionTimeout,new Watcher {
    override def process(watchedEvent: WatchedEvent): Unit = {
    }
  })

  override def registry(serviceName: String, serviceAddress: String): Unit = {
    val registryPath = ZKConstants.registryPath

    // Create registry node (persistent)
    if(zkClient.exists(registryPath, false) == null){
      zkClient.create(ZKConstants.registryPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
    }

    // Create service node (persistent)
    val servicePath = registryPath + "/" + serviceName
    if(zkClient.exists(servicePath,false) == null){
      zkClient.create(servicePath,null,Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
    }

    // Create address node (temp)
    val serviceAddress = servicePath +"/" + localhostServiceAddress
    if(zkClient.exists(serviceAddress,false) == null){
      zkClient.create(serviceAddress, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL)
    }

  }

}

