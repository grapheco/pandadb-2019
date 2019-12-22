import cn.pandadb.network.{NodeAddress, ZKPathConfig}
import cn.pandadb.server.ZKServiceRegistry
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.junit.runners.MethodSorters
import org.junit.{Assert, FixMethodOrder, Test}

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created in 14:25 2019/11/26
  * @Modified By:
  */

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
class ZKResistryTest {

//  val configFile = new File("./src/test/resources/test_pnode0.conf")
  val localNodeAddress = "10.0.88.11:1111"
  val zkServerAddress = "10.0.86.26:2181"

  val ordinaryNodePath = ZKPathConfig.ordinaryNodesPath + s"/" + localNodeAddress
  val leaderNodePath = ZKPathConfig.leaderNodePath + s"/" + localNodeAddress

  val curator: CuratorFramework = CuratorFrameworkFactory.newClient(zkServerAddress,
    new ExponentialBackoffRetry(1000, 3));
  curator.start()
  val ordinadyNodeRegistry = new ZKServiceRegistry(zkServerAddress)
  val leaderNodeRegistry = new ZKServiceRegistry(zkServerAddress)

  // no ordinaryNode before registry.
  @Test
  def test1(): Unit = {
    val flag = curator.checkExists().forPath(ordinaryNodePath)
    Assert.assertEquals(true, flag == null)
  }

  // exist ordinaryNode after registry
  @Test
  def test2(): Unit = {
    ordinadyNodeRegistry.registerAsOrdinaryNode(NodeAddress.fromString(localNodeAddress))
    val flag = curator.checkExists().forPath(ordinaryNodePath)
    Assert.assertEquals(true, flag != null)
    val ordinaryNodeAddress = curator.getChildren().forPath(ZKPathConfig.ordinaryNodesPath) // returned type is ArrayList[String]
    Assert.assertEquals("10.0.88.11:1111", ordinaryNodeAddress.get(0))
    ordinadyNodeRegistry.curator.close()
  }

  // ordinaryNode is deleted after curator_session
  @Test
  def test3(): Unit = {
    val flag = curator.checkExists().forPath(ordinaryNodePath)
    Assert.assertEquals(true, flag == null)
  }

  // no leader node before regisried as leader node
  @Test
  def test4(): Unit = {
    val flag = curator.checkExists().forPath(leaderNodePath)
    Assert.assertEquals(true, flag == null)
  }

  // exist leader node after registry
  @Test
  def test5(): Unit = {
    leaderNodeRegistry.registerAsLeader(NodeAddress.fromString(localNodeAddress))
    val flag = curator.checkExists().forPath(leaderNodePath)
    Assert.assertEquals(true, flag != false)
    leaderNodeRegistry.curator.close()
  }

  // leader node is deleted after curator session closed.
  @Test
  def test6(): Unit = {
    val flag = curator.checkExists().forPath(leaderNodePath)
    Assert.assertEquals(true, flag == null)
  }

}
