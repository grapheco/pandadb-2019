import java.io.FileInputStream
import java.net.InetAddress
import java.util.Properties

import cn.graiph.cnode.GNodeLauncher
import org.junit.{Assert, Test}

class LauncherTest {
  @Test
  def testGNodeLauncher(): Unit ={
    val serialNum =1
    val dbPath = s"./output/testdb${serialNum}"
    val confPath = s"./testdata/gnode${serialNum}.conf"
    val gNodeLauncher = new GNodeLauncher(dbPath, confPath)
    gNodeLauncher.startServer()
  }
}
