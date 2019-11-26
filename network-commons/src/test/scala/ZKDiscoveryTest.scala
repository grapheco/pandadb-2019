import java.io.File

import cn.pandadb.context.Neo4jConfigUtils
import cn.pandadb.network._
import cn.pandadb.server.ZKServiceRegistry
import cn.pandadb.util.ConfigUtils
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

class FakeListener() {
  var CHILD_ADDED = 0
  var CHILD_REMOVED = 0
  var path = s"";
}

class TestZKServiceDiscovery(curator: CuratorFramework, zkConstants: ZKConstants, listenerList: List[FakeListener]) {

  val nodesChildrenCache = new PathChildrenCache(curator, zkConstants.ordinaryNodesPath, true)
  nodesChildrenCache.start(StartMode.POST_INITIALIZED_EVENT)
  nodesChildrenCache.getListenable().addListener(
    new PathChildrenCacheListener {
      override def childEvent(curatorFramework: CuratorFramework, pathChildrenCacheEvent: PathChildrenCacheEvent): Unit = {
        try {
          pathChildrenCacheEvent.getType() match {
            case PathChildrenCacheEvent.Type.CHILD_ADDED =>
              for (listener <- listenerList) {
                listener.CHILD_ADDED = 1;
                listener.path = pathChildrenCacheEvent.getData.getPath
              }
//              for (listener <- listenerList) listener.onEvent(NodeConnected(NodeAddress.fromString(pathChildrenCacheEvent.getData.getPath)));
            case PathChildrenCacheEvent.Type.CHILD_REMOVED =>
              for (listener <- listenerList) {
                listener.CHILD_REMOVED = 1;
                listener.path = pathChildrenCacheEvent.getData.getPath
              }
//              for (listener <- listenerList) listener.onEvent(NodeDisconnected(NodeAddress.fromString(pathChildrenCacheEvent.getData.getPath)));
            // What to do if a node's data is updated?
            case PathChildrenCacheEvent.Type.CHILD_UPDATED => ;
            case _ => ;
          }
        } catch { case ex: Exception => ex.printStackTrace() }
      }
    })

}

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
class ZKDiscoveryTest {

  val configFile = new File(this.getClass.getClassLoader.getResource("test_pnode0.conf").getPath)
  val neo4jConfig = Config.builder().withFile(configFile).build()
  val pandaConfig = Neo4jConfigUtils.neo4jConfig2Config(neo4jConfig)
  val pandaConfigEX = ConfigUtils.config2Ex(pandaConfig)
  val zkConstants = new ZKConstants(pandaConfigEX)

  val curator: CuratorFramework = CuratorFrameworkFactory.newClient(zkConstants.zkServerAddress,
    new ExponentialBackoffRetry(1000, 3));
  curator.start()

  val listenerList: List[FakeListener] = List(new FakeListener, new FakeListener)
  val ordinadyNodeRegistry = new ZKServiceRegistry(zkConstants)
  val testZKServiceDiscovery = new TestZKServiceDiscovery(curator, zkConstants, listenerList)

  //emptyListenerList
  @Test
  def test1(): Unit = {
    for (listener <- listenerList) {
      Assert.assertEquals(0, listener.CHILD_ADDED)
      Assert.assertEquals(0, listener.CHILD_REMOVED)
    }
  }

  //discoverRegister
  @Test
  def test2(): Unit = {
    ordinadyNodeRegistry.registerAsOrdinaryNode(zkConstants.localNodeAddress)
    for (listener <- listenerList) {
      Assert.assertEquals(1, listener.CHILD_ADDED)
      Assert.assertEquals(0, listener.CHILD_REMOVED)
    }
  }


  //discoverUnRegister
  @Test
  def test3(): Unit = {
    ordinadyNodeRegistry.curator.close()
    for (listener <- listenerList) {
      Assert.assertEquals(1, listener.CHILD_ADDED)
      Assert.assertEquals(1, listener.CHILD_REMOVED)
    }
  }



}
