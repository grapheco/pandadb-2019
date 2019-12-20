package release

import java.io.{File, FileInputStream}
import java.util.Properties

import cn.pandadb.network.{NodeAddress, ZKPathConfig}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.junit.{Assert, Test}
import EnvironmentTest.{clusterNodes, curator}
import org.neo4j.driver.{AuthTokens, GraphDatabase}

/**
  * @Author: Airzihao
  * @Description: This is a comprehensive test for the environment.
  * @Date: Created at 10:24 2019/12/20
  * @Modified By:
  */

object EnvironmentTest {

  val dir = new File("./itest/comprehensive")
  if (!dir.exists()) {
    dir.mkdirs()
  }
  val props: Properties = {
    val props = new Properties()
    props.load(new FileInputStream(new File(s"${dir}/envirTest.properties")))
    props
  }
  val zkString = props.getProperty("zkServerAddr")
  val clusterNodes: Array[NodeAddress] = {
    props.getProperty("clusterNodes").split(",").map(str => NodeAddress.fromString(str))
  }
  val curator: CuratorFramework = CuratorFrameworkFactory.newClient(zkString,
    new ExponentialBackoffRetry(1000, 3));
  curator.start()
}

// shall this class be established on the driver side?
class EnvironmentTest {
  // test zk environment
  @Test
  def test1(): Unit = {
    if (curator.checkExists().forPath(ZKPathConfig.registryPath) == null) {
      curator.create().forPath(ZKPathConfig.registryPath)
    }
    curator.delete().deletingChildrenIfNeeded().forPath(ZKPathConfig.registryPath)
    Assert.assertEquals(null, curator.checkExists().forPath(ZKPathConfig.registryPath))
  }

  // make sure the driver can access to each node.
  @Test
  def test2(): Unit = {
    clusterNodes.foreach(nodeAddress => {
      val boltURI = s"bolt://${nodeAddress.getAsString}"
      val driver = GraphDatabase.driver(boltURI, AuthTokens.basic("", ""))
      val session = driver.session()
      val tx = session.beginTransaction()
      tx.success()
      tx.close()
      session.close()
      driver.close()
    })
  }

  // how to make sure the node has access to each other?
  @Test
  def test3(): Unit = {

  }
}
