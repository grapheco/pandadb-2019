import java.io.File

import org.neo4j.kernel.configuration.Config
import cn.pandadb.context.Neo4jConfigUtils
import cn.pandadb.network.{ZKConstants, ZKPathConfig}
import cn.pandadb.util._
import org.junit.{Assert, Test}

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created in 11:07 2019/11/26
  * @Modified By:
  */
class ZKConstantsTest {
  val configFile = new File(this.getClass.getClassLoader.getResource("test_pnode0.conf").getPath)
  val neo4jConfig = Config.builder().withFile(configFile).build()
  val pandaConfig = Neo4jConfigUtils.neo4jConfig2Config(neo4jConfig)
  val pandaConfigEX = ConfigUtils.config2Ex(pandaConfig)
  val zkConstants = new ZKConstants(pandaConfigEX)

  @Test
  def testZKPathConfig(): Unit = {
    Assert.assertEquals(s"/pandaNodes", ZKPathConfig.registryPath)
    Assert.assertEquals(s"/pandaNodes/leaderNode", ZKPathConfig.leaderNodePath)
    Assert.assertEquals(s"/pandaNodes/ordinaryNodes", ZKPathConfig.ordinaryNodesPath)
  }
  @Test
  def testZkConstant(): Unit = {
    Assert.assertEquals(zkConstants.connectionTimeout, 10000)
    Assert.assertEquals(zkConstants.zkServerAddress, "10.0.86.26:2181,10.0.86.27:2181,10.0.86.70:2181")
    Assert.assertEquals(zkConstants.localNodeAddress, "10.0.88.11:1111")
  }
}
