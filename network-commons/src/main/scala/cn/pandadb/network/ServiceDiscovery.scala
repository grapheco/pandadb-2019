package cn.pandadb.network

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode
import org.apache.curator.framework.recipes.cache.{PathChildrenCache, PathChildrenCacheEvent, PathChildrenCacheListener}
/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created in 22:54 2019/11/25
  * @Modified By:
  */


trait ServiceDiscovery {

}

class ZKServiceDiscovery(curator: CuratorFramework, zkConstants: ZKConstants, listenerList: List[ZKClusterEventListener]) {

  val nodesChildrenCache = new PathChildrenCache(curator, ZKPathConfig.ordinaryNodesPath, true)
  nodesChildrenCache.start(StartMode.POST_INITIALIZED_EVENT)
  nodesChildrenCache.getListenable().addListener(
    new PathChildrenCacheListener {
      override def childEvent(curatorFramework: CuratorFramework, pathChildrenCacheEvent: PathChildrenCacheEvent): Unit = {
        try {
          pathChildrenCacheEvent.getType() match {
            case PathChildrenCacheEvent.Type.CHILD_ADDED =>
              for (listener <- listenerList) listener.onEvent(NodeConnected(NodeAddress.fromString(pathChildrenCacheEvent.getData.getPath)));
            case PathChildrenCacheEvent.Type.CHILD_REMOVED =>
              for (listener <- listenerList) listener.onEvent(NodeDisconnected(NodeAddress.fromString(pathChildrenCacheEvent.getData.getPath)));
            // What to do if a node's data is updated?
            case PathChildrenCacheEvent.Type.CHILD_UPDATED => ;
          }
        } catch { case ex: Exception => ex.printStackTrace() }
      }
    })

}



