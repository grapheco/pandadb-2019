import cn.pandadb.network.ZKPathConfig
import org.junit.{Assert, Test}

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created in 11:07 2019/11/26
  * @Modified By:
  */
class ZKConstantsTest {

  @Test
  def testZKPathConfig(): Unit = {
    Assert.assertEquals(s"/PandaDB-v0.0.2", ZKPathConfig.registryPath)
    Assert.assertEquals(s"/PandaDB-v0.0.2/leaderNode", ZKPathConfig.leaderNodePath)
    Assert.assertEquals(s"/PandaDB-v0.0.2/ordinaryNodes", ZKPathConfig.ordinaryNodesPath)
  }
}
