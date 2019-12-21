import java.io.File
import java.util.concurrent.Executors

import cn.pandadb.network._
import cn.pandadb.server.ZKServiceRegistry
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode
import org.apache.curator.framework.recipes.cache.{PathChildrenCache, PathChildrenCacheEvent, PathChildrenCacheListener}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.junit.runners.MethodSorters
import org.junit.{Assert, FixMethodOrder, Test}
import org.neo4j.kernel.configuration.Config

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created in 17:02 2019/11/26
  * @Modified By:
  */

class FakeListener(listenerId: Int) {
  val id = listenerId
  var CHILD_ADDED = 0
  var CHILD_REMOVED = 0
  var path = s"";
}

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
class ZKDiscoveryTest {

  //FIXME: use base class to init
  val configFile = new File(this.getClass.getClassLoader.getResource("test_pnode0.conf").getPath)
  val zkConstants = ZKConstants

  val curator: CuratorFramework = CuratorFrameworkFactory.newClient(zkConstants.zkServerAddress,
    new ExponentialBackoffRetry(1000, 3));
  curator.start()

  val listenerList: List[FakeListener] = List(new FakeListener(1), new FakeListener(2))
  val ordinadyNodeRegistry = new ZKServiceRegistry(zkConstants.zkServerAddress)
  testZKServiceDiscovery(curator, zkConstants, listenerList)

  var funcNum = 0

  @Test
  def test0(): Unit = {

    funcNum = 1
    for (listener <- listenerList) {
      Assert.assertEquals(0, listener.CHILD_ADDED)
      Assert.assertEquals(0, listener.CHILD_REMOVED)
      Assert.assertEquals("", listener.path)
    }
    funcNum = 11

    funcNum = 2
    ordinadyNodeRegistry.registerAsOrdinaryNode(NodeAddress.fromString(zkConstants.localNodeAddress))
    Thread.sleep(1000)
    for (listener <- listenerList) {
      Assert.assertEquals(1, listener.CHILD_ADDED)
      Assert.assertEquals(0, listener.CHILD_REMOVED)
      Assert.assertEquals("10.0.88.11:1111", listener.path)
    }
    funcNum = 22

    funcNum = 3
    ordinadyNodeRegistry.unRegisterOrdinaryNode(NodeAddress.fromString(zkConstants.localNodeAddress))
    Thread.sleep(1000)

    for (listener <- listenerList) {
      Assert.assertEquals(1, listener.CHILD_ADDED)
      Assert.assertEquals(1, listener.CHILD_REMOVED)
    }
    funcNum = 33
  }

  def testZKServiceDiscovery(curator: CuratorFramework, zkConstants: ZKConstants.type, listenerList: List[FakeListener]) {

    val nodesChildrenCache = new PathChildrenCache(curator, ZKPathConfig.ordinaryNodesPath, false)

    //caution: use sync method. POST_INITIAL_EVENT is an async method.
    nodesChildrenCache.start(StartMode.BUILD_INITIAL_CACHE)

    val listener = new PathChildrenCacheListener {
      override def childEvent(curatorFramework: CuratorFramework, pathChildrenCacheEvent: PathChildrenCacheEvent): Unit = {
        try {
          pathChildrenCacheEvent.getType() match {
            case PathChildrenCacheEvent.Type.CHILD_ADDED =>
              for (listener <- listenerList) {
                listener.CHILD_ADDED = 1;
                // if not splitted, returned: /pandaNodes/ordinaryNodes.10.0.88.11:1111
                listener.path = pathChildrenCacheEvent.getData.getPath.split(s"/").last
              }

            case PathChildrenCacheEvent.Type.CHILD_REMOVED =>
              for (listener <- listenerList) {
                listener.CHILD_REMOVED = 1;
                listener.path = pathChildrenCacheEvent.getData.getPath
              }
            // What to do if a node's data is updated?
            case PathChildrenCacheEvent.Type.CHILD_UPDATED => ;
            case _ => ;
          }
        } catch { case ex: Exception => ex.printStackTrace() }
      }
    }
    nodesChildrenCache.getListenable().addListener(listener)
  }

}
