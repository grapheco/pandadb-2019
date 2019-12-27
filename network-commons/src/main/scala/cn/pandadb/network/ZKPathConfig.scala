package cn.pandadb.network

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.{CreateMode, ZooDefs}

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created in 20:41 2019/11/26
  * @Modified By:
  */
object ZKPathConfig {
  val registryPath = s"/PandaDB-v0.0.2"
  val ordinaryNodesPath = registryPath + s"/ordinaryNodes"
  val leaderNodePath = registryPath + s"/leaderNode"
  val dataVersionPath = registryPath + s"/version"
  val freshNodePath = registryPath + s"/freshNode"

  def initZKPath(zkString: String): Unit = {
    val _curator = CuratorFrameworkFactory.newClient(zkString,
      new ExponentialBackoffRetry(1000, 3))
    _curator.start()
    val list = List(registryPath, ordinaryNodesPath, leaderNodePath, dataVersionPath, freshNodePath)
    list.foreach(path => {
      if (_curator.checkExists().forPath(path) == null) {
        _curator.create()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.PERSISTENT)
          .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
          .forPath(path)
      }
    })
    _curator.close()
  }
}
