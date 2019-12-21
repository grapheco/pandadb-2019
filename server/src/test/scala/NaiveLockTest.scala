import java.io.File

import cn.pandadb.network.{NodeAddress, ZKConstants, ZookeeperBasedClusterClient}
import cn.pandadb.server.{MasterRole, ZKServiceRegistry}
import org.junit.runners.MethodSorters
import org.junit.{Assert, FixMethodOrder, Test}
import org.neo4j.kernel.configuration.Config

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created at 9:06 2019/11/28
  * @Modified By:
  */

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
class NaiveLockTest {

  val configFile = new File(this.getClass.getClassLoader.getResource("test_pnode0.conf").getPath)
  val zkConstants = ZKConstants

  val zkString = zkConstants.zkServerAddress
  val clusterClient = new ZookeeperBasedClusterClient(zkString)
  val master = new MasterRole(clusterClient, NodeAddress.fromString(zkConstants.localNodeAddress))

  val register = new ZKServiceRegistry(zkString)

  val nodeList = List("10.0.88.11:1111", "10.0.88.22:2222", "10.0.88.33:3333", "10.0.88.44:4444")

  //register nodes
  @Test
  def test1(): Unit = {
    Assert.assertEquals(true, clusterClient.getAllNodes().isEmpty)
    Assert.assertEquals(true, clusterClient.getWriteMasterNode("").isEmpty)

    nodeList.foreach(nodeStr => register.registerAsOrdinaryNode(NodeAddress.fromString(nodeStr)))
    register.registerAsLeader(NodeAddress.fromString(nodeList.head))

    Assert.assertEquals(nodeList.head, clusterClient.getWriteMasterNode("").get.getAsString)
    val a = clusterClient.getAllNodes().map(_.getAsString).toList
    Assert.assertEquals(true, compareList(nodeList, clusterClient.getAllNodes()))

  }

  // test write lock
  @Test
  def test2(): Unit = {
    master.globalWriteLock.lock()
    Assert.assertEquals(true, clusterClient.getAllNodes().isEmpty)
    Assert.assertEquals(true, clusterClient.getWriteMasterNode("").isEmpty)
    master.globalWriteLock.unlock()
    Assert.assertEquals(nodeList.head, clusterClient.getWriteMasterNode("").get.getAsString)
    Assert.assertEquals(true, compareList(nodeList, clusterClient.getAllNodes()))
  }

  // test read lock
  @Test
  def test3(): Unit = {
    master.globalReadLock.lock()
    Thread.sleep(3000)
    Assert.assertEquals(true, clusterClient.getAllNodes().isEmpty)
    Assert.assertEquals(nodeList.head, clusterClient.getWriteMasterNode("").get.getAsString)
    master.globalReadLock.unlock()
    Thread.sleep(3000)
    Assert.assertEquals(nodeList.head, clusterClient.getWriteMasterNode("").get.getAsString)
    Assert.assertEquals(true, compareList(nodeList, clusterClient.getAllNodes()))

  }

  def compareList(srtList: List[String], allNodes: Iterable[NodeAddress]): Boolean = {
    allNodes.map(_.getAsString).toSet.equals(srtList.toSet)
  }

}
