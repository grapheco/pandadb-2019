import java.io.File

import cn.pandadb.context.Neo4jConfigUtils
import cn.pandadb.network.ZKConstants
import cn.pandadb.server.ZKServiceRegistry
import cn.pandadb.util.ConfigUtils
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.junit.runners.MethodSorters
import org.junit.{After, Assert, FixMethodOrder, Test}
import org.neo4j.kernel.configuration.Config

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created in 14:25 2019/11/26
  * @Modified By:
  */

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
class ZKResistryTest {

  val configFile = new File(this.getClass.getClassLoader.getResource("test_pnode0.conf").getPath)
  val neo4jConfig = Config.builder().withFile(configFile).build()
  val pandaConfig = Neo4jConfigUtils.neo4jConfig2Config(neo4jConfig)
  val pandaConfigEX = ConfigUtils.config2Ex(pandaConfig)
  val zkConstants = new ZKConstants(pandaConfigEX)

  val ordinaryNodePath = zkConstants.ordinaryNodesPath + s"/" + zkConstants.localNodeAddress
  val leaderNodePath = zkConstants.leaderNodePath + s"/" + zkConstants.localNodeAddress

  val curator: CuratorFramework = CuratorFrameworkFactory.newClient(zkConstants.zkServerAddress,
    new ExponentialBackoffRetry(1000, 3));
  curator.start()
  val ordinadyNodeRegistry = new ZKServiceRegistry(zkConstants)
  val leaderNodeRegistry = new ZKServiceRegistry(zkConstants)

  // no ordinaryNode before registry.
  @Test
  def test1(): Unit = {
    val flag = curator.checkExists().forPath(ordinaryNodePath)
    Assert.assertEquals(true, flag == null)
  }

  // exist ordinaryNode after registry
  @Test
  def test2(): Unit = {
    ordinadyNodeRegistry.registerAsOrdinaryNode(zkConstants.localNodeAddress)
    val flag = curator.checkExists().forPath(ordinaryNodePath)
    Assert.assertEquals(true, flag != null)
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
    leaderNodeRegistry.registerAsLeader(leaderNodePath)
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
