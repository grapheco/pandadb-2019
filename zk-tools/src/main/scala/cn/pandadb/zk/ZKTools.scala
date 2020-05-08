package cn.pandadb.zk

import scala.collection.JavaConverters._
import cn.pandadb.configuration.Config
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.data.Stat
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



}