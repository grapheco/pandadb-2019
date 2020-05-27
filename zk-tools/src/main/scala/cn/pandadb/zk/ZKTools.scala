package cn.pandadb.zk

import java.text.MessageFormat

import scala.collection.JavaConverters._
import cn.pandadb.configuration.Config
import org.apache.curator.framework.recipes.cache.{ChildData, NodeCache, NodeCacheListener, PathChildrenCache, PathChildrenCacheListener, TreeCache, TreeCacheListener}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.{CreateMode, ZooDefs}
import org.slf4j.Logger

class ZKTools(config: Config) {
  val logger: Logger = config.getLogger(this.getClass)

  private val zkServerAddress: String = config.getZKAddress()
  private val pandaZkDir: String = config.getPandaZKDir.replaceAll("/{2,}", "/")
  private var curator: CuratorFramework = null

  def init(): Unit = {
    curator = CuratorFrameworkFactory.newClient(zkServerAddress, new ExponentialBackoffRetry(1000, 3))
    curator.start()
  }

  def close(): Unit = {
    logger.info(this.getClass + ": stop")
    curator.close()
  }

  def getCurator(): CuratorFramework = {
    if (curator == null) {
      throw new Exception("Curator is not init before use")
    }
    curator
  }

  def buildFullZKPath(path: String): String = {
    (pandaZkDir + "/" + path).replaceAll("/{2,}", "/")
  }

  def createZKNode(mode: CreateMode, path: String, content: String): String = {
      curator.create().creatingParentsIfNeeded.withMode(mode).forPath(path, content.getBytes("UTF-8"))
  }

  def createZKNode(mode: CreateMode, path: String): String = {
    curator.create().creatingParentsIfNeeded.withMode(mode).forPath(path)
  }

  def deleteZKNodeAndChildren(path: String): Unit = {
    curator.delete().deletingChildrenIfNeeded().forPath(path)
  }

  def checkChildExist(path: String, child: String): Boolean = {
    if (curator.checkExists().forPath(path + "/" + child) == null) false else true
  }

  def checkPathExist(path: String): Boolean = {
    if (curator.checkExists().forPath(path) == null) false else true
  }

  def getZKNodeChildren(path: String): List[String] = {
    curator.getChildren().forPath(path).asScala.toList
  }

  def getZKNodeData(path: String): String = {
    new String(curator.getData().forPath(path))
  }

  def setZKNodeData(path: String, content: String, withVersion: Int): Unit = {
    curator.setData().withVersion(withVersion)
      .forPath(path, content.getBytes("UTF-8"));
  }

  def setZKNodeData(path: String, content: String): Unit = {
    curator.setData().forPath(path, content.getBytes("UTF-8"));
  }

  def assureZKNodeExist(path: String): Unit = {
    if (curator.checkExists().forPath(path) == null) {
      curator.create()
        .creatingParentsIfNeeded()
        .withMode(CreateMode.PERSISTENT)
        .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
        .forPath(path)
    }
  }

  def assureZKNodeExist(paths: String*): Unit = {
    paths.foreach(path => {
      assureZKNodeExist(path)
    })
  }

  def assureZKNodeExist(paths: Iterable[String]): Unit = {
    paths.foreach(path => {
      assureZKNodeExist(path)
    })
  }

  def registerPathChildrenListener(nodePath: String, listener: PathChildrenCacheListener): PathChildrenCache = {
    try {
      val pathChildrenCache = new PathChildrenCache(curator, nodePath, true)
      pathChildrenCache.getListenable.addListener(listener)
      pathChildrenCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE)
      return pathChildrenCache
    } catch {
      case e: Exception =>
        logger.error(e.getMessage)
        throw e
    }
    null
  }

  def registerPathChildrenListener(pathChildrenCache: PathChildrenCache, listener: PathChildrenCacheListener): PathChildrenCache = {
    try {
      pathChildrenCache.getListenable.addListener(listener)
      return pathChildrenCache
    } catch {
      case e: IllegalStateException =>
        logger.error(e.getMessage)
      case e: Exception =>
        logger.error(e.getMessage)
        throw e
    }
    null
  }

  def registerTreeCacheListener(nodePath: String, maxDepth: Int, listener: TreeCacheListener): TreeCache = {
    try {
      val treeCache = TreeCache.newBuilder(curator, nodePath).setCacheData(true).setMaxDepth(maxDepth).build
      treeCache.getListenable.addListener(listener)
      treeCache.start
      return treeCache
    } catch {
      case e: Exception =>
        logger.error(e.getMessage)
        throw e
    }
    null
  }

  def registerTreeCacheListener(treeCache: TreeCache, maxDepth: Int, listener: TreeCacheListener): TreeCache = {
    try {
      treeCache.getListenable.addListener(listener)
      treeCache.start
      return treeCache
    } catch {
      case e: IllegalStateException =>
        logger.error(e.getMessage)
      case e: Exception =>
        logger.error(e.getMessage)
        throw e
    }
    null
  }

  def registerNodeCacheListener(nodePath: String, listener: NodeCacheListener): NodeCache = {
    try {
      val nodeCache = new NodeCache(curator, nodePath)
      nodeCache.getListenable.addListener(listener)
      nodeCache.start()
      return nodeCache
    } catch {
      case e: Exception =>
        logger.error(e.getMessage)
        throw e
    }
    null
  }

  def registerNodeCacheListener(nodeCache: NodeCache, listener: NodeCacheListener): NodeCache = {
    try {
      nodeCache.getListenable.addListener(listener)
      nodeCache.start()
      return nodeCache
    } catch {
      case e: IllegalStateException =>
        logger.error(e.getMessage)
      case e: Exception =>
        logger.error(e.getMessage)
        throw e
    }
    null
  }

}