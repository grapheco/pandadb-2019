package cn.pandadb.network

import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.{CreateMode, ZooDefs}

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created in 20:41 2019/11/26
  * @Modified By:
  */
object ZKPathConfig {
  //object
  val registryPath = s"/testPandaDB"
  val ordinaryNodesPath = registryPath + s"/ordinaryNodes"
  val leaderNodePath = registryPath + s"/leaderNode"
  val dataVersionPath = registryPath + s"/version"
  val freshNodePath = registryPath + s"/freshNode"

  def initZKPath(curator: CuratorFramework): Unit = {
    val list = List(registryPath, ordinaryNodesPath, leaderNodePath, dataVersionPath, freshNodePath)
    list.foreach(path => {
      if (curator.checkExists().forPath(path) == null) {
        curator.create()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.PERSISTENT)
          .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
          .forPath(path)
      }
    })
  }
}
