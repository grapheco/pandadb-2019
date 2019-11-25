package cn.pandadb.server
import cn.pandadb.network.ZKConstants
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.{PathChildrenCache, PathChildrenCacheEvent, PathChildrenCacheListener}
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode
/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created in 22:54 2019/11/25
  * @Modified By:
  */


trait ServiceDiscovery {

}

class ZKServiceDiscovery(curator: CuratorFramework, zkConstants: ZKConstants) {
  val nodesChildrenCache = new PathChildrenCache(curator, zkConstants.ordinaryNodesPath, true)
  nodesChildrenCache.start(StartMode.POST_INITIALIZED_EVENT)
  nodesChildrenCache.getListenable().addListener(
    new PathChildrenCacheListener {
      override def childEvent(curatorFramework: CuratorFramework, pathChildrenCacheEvent: PathChildrenCacheEvent): Unit = {
        try {
          pathChildrenCacheEvent.getType() match {
            //TODO: What to do when watch these events?
            case PathChildrenCacheEvent.Type.CHILD_ADDED => _;
            case PathChildrenCacheEvent.Type.CHILD_REMOVED => _;
            case PathChildrenCacheEvent.Type.CHILD_UPDATED => _;
          }
        } catch { case ex: Exception => ex.printStackTrace() }
      }
    })
}



