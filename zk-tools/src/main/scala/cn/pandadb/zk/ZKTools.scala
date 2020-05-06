package cn.pandadb.zk

import scala.collection.JavaConverters._

import cn.pandadb.configuration.Config
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.CreateMode
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

  def createZKNode(mode: CreateMode, path: String, content: String): Unit = {
      curator.create().creatingParentsIfNeeded.withMode(mode).forPath(path, content.getBytes("UTF-8"))
  }

  def createZKNode(mode: CreateMode, path: String): Unit = {
    curator.create().creatingParentsIfNeeded.withMode(mode).forPath(path)
  }

  def getZKNodeChildren(path: String): Array[AnyRef] = {
    curator.getChildren().forPath(path).toArray()
  }

}