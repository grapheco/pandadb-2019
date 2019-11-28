import java.io.File

import cn.pandadb.context.Neo4jConfigUtils
import cn.pandadb.network.{NodeAddress, ZKConstants, ZookeerperBasedClusterClient}
import cn.pandadb.server.ZKServiceRegistry
import cn.pandadb.util.ConfigUtils
import org.junit.runners.MethodSorters
import org.junit.{Assert, FixMethodOrder, Test}
import org.neo4j.kernel.configuration.Config

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created at 10:32 2019/11/27
  * @Modified By:
  */

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
class ZookeeperBasedClusterClientTest {

  val configFile = new File(this.getClass.getClassLoader.getResource("test_pnode0.conf").getPath)
  val neo4jConfig = Config.builder().withFile(configFile).build()
  val pandaConfig = Neo4jConfigUtils.neo4jConfig2Config(neo4jConfig)
  val pandaConfigEX = ConfigUtils.config2Ex(pandaConfig)
  val zkConstants = new ZKConstants(pandaConfigEX)

  val zkString = zkConstants.zkServerAddress

  val clusterClient = new ZookeerperBasedClusterClient(zkString)
  val register = new ZKServiceRegistry(zkString)

  // empty at first
  @Test
  def test1(): Unit = {
    Assert.assertEquals(true, clusterClient.getAllNodes().isEmpty)
  }

  // getAllNodes, will get test node
  @Test
  def test2(): Unit = {
    register.registerAsOrdinaryNode(zkConstants.localNodeAddress)
    Thread.sleep(1000)
    Assert.assertEquals(false, clusterClient.getAllNodes().isEmpty)
    Assert.assertEquals(NodeAddress.fromString("10.0.88.11:1111"), clusterClient.getAllNodes().iterator.next())
  }

  // empty after test node unRegister itself
  @Test
  def test3(): Unit = {
    register.unRegisterOrdinaryNode(zkConstants.localNodeAddress)
    Assert.assertEquals(true, clusterClient.getAllNodes().isEmpty)
  }

  // test leader
  @Test
  def test4(): Unit = {
    register.registerAsLeader(zkConstants.localNodeAddress)
    Thread.sleep(1000)
    Assert.assertEquals(NodeAddress.fromString("10.0.88.11:1111"), clusterClient.getWriteMasterNode().get)
  }

}
