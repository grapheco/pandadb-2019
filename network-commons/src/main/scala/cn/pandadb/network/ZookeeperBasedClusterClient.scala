package cn.pandadb.network

import scala.collection.JavaConverters._
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode
import org.apache.curator.framework.recipes.cache.{PathChildrenCache, PathChildrenCacheEvent, PathChildrenCacheListener}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.{CreateMode, ZooDefs}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created in 9:06 2019/11/26
  * @Modified By:
  */

class ZookeeperBasedClusterClient(zkString: String) extends ClusterClient {

  val zkServerAddress = zkString
  val curator: CuratorFramework = CuratorFrameworkFactory.newClient(zkServerAddress,
    new ExponentialBackoffRetry(1000, 3));
  curator.start()

  private var currentState: ClusterState = _

  var listenerList: List[ZKClusterEventListener] = List[ZKClusterEventListener]()

  // query from zk when init.
  private var availableNodes: Set[NodeAddress] = {
    val pathArrayList = curator.getChildren.forPath(ZKPathConfig.ordinaryNodesPath).asScala
    pathArrayList.map(NodeAddress.fromString(_)).toSet
  }

  // add listener, to monitor zk nodes change
  addCuratorListener()

  override def getWriteMasterNode(): NodeAddress = {
    var leaderAddress = curator.getChildren().forPath(ZKPathConfig.leaderNodePath)

    while (leaderAddress.isEmpty) {
      Thread.sleep(500)
      leaderAddress = curator.getChildren().forPath(ZKPathConfig.leaderNodePath)
    }
    NodeAddress.fromString(leaderAddress.get(0))
  }

  def getWriteMasterNode(inner: String): Option[NodeAddress] = {
    val leaderAddress = curator.getChildren().forPath(ZKPathConfig.leaderNodePath)

    if(leaderAddress.isEmpty) {
      None
    } else {
      Some(NodeAddress.fromString(leaderAddress.get(0)))
    }
  }

  // return variable availableNodes, don't query from zk every time.
  override def getAllNodes(): Iterable[NodeAddress] = {
    availableNodes
  }

  override def getCurrentState(): ClusterState = {
    currentState
  }

  // add listener to listenerList, of no use at this period.
  override def listen(listener: ClusterEventListener): Unit = {
    listenerList = listener.asInstanceOf[ZKClusterEventListener] :: listenerList
  }

  override def waitFor(state: ClusterState): Unit = null

  def getCurator(): CuratorFramework = {
    curator
  }

  def getFreshNodeIp(): String = {
    curator.getChildren.forPath(ZKPathConfig.freshNodePath).get(0)
  }

  def getClusterDataVersion(): Int = {
    if (curator.checkExists().forPath(ZKPathConfig.dataVersionPath) == null) {
      curator.create()
        .creatingParentsIfNeeded()
        .withMode(CreateMode.PERSISTENT)
        .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
        .forPath(ZKPathConfig.dataVersionPath)
      curator.setData().forPath(ZKPathConfig.dataVersionPath, BytesTransform.serialize(-1))
      BytesTransform.deserialize(curator.getData.forPath(ZKPathConfig.dataVersionPath))
    } else {
      // stat.version == 0, means not init
      val stat = new Stat()
      val version = curator.getData.storingStatIn(stat).forPath(ZKPathConfig.dataVersionPath)
      if (stat.getVersion == 0) {
        curator.setData().forPath(ZKPathConfig.dataVersionPath, BytesTransform.serialize(-1))
        BytesTransform.deserialize(curator.getData.forPath(ZKPathConfig.dataVersionPath))
      } else {
        BytesTransform.deserialize(version)
      }
    }
  }

  def addCuratorListener(): Unit = {

    val nodesChildrenCache = new PathChildrenCache(curator, ZKPathConfig.ordinaryNodesPath, true)
    nodesChildrenCache.start(StartMode.BUILD_INITIAL_CACHE)
    nodesChildrenCache.getListenable().addListener(
      new PathChildrenCacheListener {
        override def childEvent(curatorFramework: CuratorFramework, pathChildrenCacheEvent: PathChildrenCacheEvent): Unit = {
          try {
            pathChildrenCacheEvent.getType() match {

              case PathChildrenCacheEvent.Type.CHILD_ADDED =>
                val nodeAddress = NodeAddress.fromString(pathChildrenCacheEvent.getData.getPath.split(s"/").last)
                availableNodes += nodeAddress
                for (listener <- listenerList) listener.onEvent(NodeConnected(nodeAddress));
              case PathChildrenCacheEvent.Type.CHILD_REMOVED =>
                val nodeAddress = NodeAddress.fromString(pathChildrenCacheEvent.getData.getPath.split(s"/").last)
                availableNodes -= nodeAddress
                for (listener <- listenerList) listener.onEvent(NodeDisconnected(NodeAddress.fromString(pathChildrenCacheEvent.getData.getPath)));

              // What to do if a node's data is updated?
              case PathChildrenCacheEvent.Type.CHILD_UPDATED => ;
              case _ => ;
            }
          } catch { case ex: Exception => ex.printStackTrace() }
        }
      })
  }

}
