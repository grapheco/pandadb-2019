package cn.pandadb.network

import scala.collection.JavaConverters._
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode
import org.apache.curator.framework.recipes.cache.{PathChildrenCache, PathChildrenCacheEvent, PathChildrenCacheListener}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created in 9:06 2019/11/26
  * @Modified By:
  */

class ZookeerperBasedClusterClient(zkString: String) extends ClusterClient {

  val zkServerAddress = zkString
  val curator: CuratorFramework = CuratorFrameworkFactory.newClient(zkServerAddress,
    new ExponentialBackoffRetry(1000, 3));
  curator.start()

  // private, avoid outter write
  private var currentState: ClusterState = _

  var listenerList: List[ZKClusterEventListener] = List[ZKClusterEventListener]()

  // query from zk when init.
  private var availableNodes: Set[NodeAddress] = {
    val pathArrayList = curator.getChildren.forPath(ZKPathConfig.ordinaryNodesPath).asScala
    pathArrayList.map(NodeAddress.fromString(_)).toSet
  }

  // add listener, to monitor zk nodes change
  addCuratorListener()

  // silly while wait? what an ugly implemention!
  override def getWriteMasterNode(): NodeAddress = {
    var leaderAddress = curator.getChildren().forPath(ZKPathConfig.leaderNodePath)

    while (leaderAddress.isEmpty) {
      Thread.sleep(1000)
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
                // is this sentence useful?
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
