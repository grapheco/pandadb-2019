import java.io.File

import cn.pandadb.network.{ZKConstants, ZKPathConfig}
import org.junit.{Assert, Test}

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created in 11:07 2019/11/26
  * @Modified By:
  */
class ZKConstantsTest {
  val configFile = new File(this.getClass.getClassLoader.getResource("test_pnode0.conf").getPath)
  val zkConstants = ZKConstants

  @Test
  def testZKPathConfig(): Unit = {
    Assert.assertEquals(s"/pandaNodes", ZKPathConfig.registryPath)
    Assert.assertEquals(s"/pandaNodes/leaderNode", ZKPathConfig.leaderNodePath)
    Assert.assertEquals(s"/pandaNodes/ordinaryNodes", ZKPathConfig.ordinaryNodesPath)
  }
  @Test
  def testZkConstant(): Unit = {
//    Assert.assertEquals(zkConstants.connectionTimeout, 10000)
    Assert.assertEquals(zkConstants.zkServerAddress, "10.0.86.26:2181,10.0.86.27:2181,10.0.86.70:2181")
    Assert.assertEquals(zkConstants.localNodeAddress, "10.0.88.11:1111")
  }
}
