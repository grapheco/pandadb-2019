import java.io.{File, FileInputStream}
import java.util.Properties

import cn.pandadb.network.NodeAddress
import org.junit.Test

import sys.process._

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created at 20:04 2019/12/25
  * @Modified By:
  */
class UnsafePNodeLauncherTest {

  @Test
  def test1(): Unit = {
    val num = 0
    val startCmd = s"cmd.exe /c mvn exec:java -Dexec.mainClass='cn.pandadb.tool.UnsafePNodeLauncher' -Dexec.args=${num}" !!;
    val confFile = new File(s"./itest/testdata/localnode${num}.conf")
    Thread.sleep(999999)
  }

}

import sys.process._
class PandaDBTestBase {
  var serialNum = 0

  def startLocalPNodeServer(): NodeAddress = {
    val startCmd = s"cmd.exe /c mvn exec:java -Dexec.mainClass='cn.pandadb.tool.UnsafePNodeLauncher' -Dexec.args=${serialNum}"
    startCmd !!;
    val localNodeAddress = _getLocalNodeAddressFromFile(new File(s"./itest/testdata/localnode${serialNum}.conf"))
    serialNum += 1;
    Thread.sleep(100000)
    localNodeAddress
  }

  // For base test's use.
  private def _getLocalNodeAddressFromFile(confFile: File): NodeAddress = {
    val props = new Properties()
    props.load(new FileInputStream(confFile))
    NodeAddress.fromString(props.getProperty("node.server.address"))
  }
}
