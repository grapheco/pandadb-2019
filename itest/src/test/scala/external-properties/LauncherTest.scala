import java.io.FileInputStream
import java.net.InetAddress
import java.util.Properties

import cn.graiph.cnode._
import org.junit.{Assert, Test}


object LauncherTest {

  def main(args: Array[String]): Unit = {

    /*
        Don't use the following three functions together.
        Because a node can only play one role.
        Hint: Fix the serviceAddress in gnode${serialNum}.conf to your real address.
     */

    // 1. change the serialNum from 1 to 2 in different IDEA
//    launchAsReadNode(1)

    // 2. change the serialNum from 3 to 4 in different IDEA
    // launchAsWriteNode(3)

    // 3. coor's serialNum is 0
//    launchAsCoordinator(0)
  }


  // for easy test only: valid serialNum value is 1,2,3,4
  def testGNodeLauncher(serialNum: Int): Unit ={
    val dbPath = s"./itest/output/testdb${serialNum}"
    val confPath = s"./itest/testdata/gnode${serialNum}.conf"
    val gNodeLauncher = new GNodeLauncher(dbPath, confPath)
    gNodeLauncher.startServer()
  }

  def launchAsReadNode(serialNum: Int): Unit = {
    val dbPath = s"./itest/output/testdb${serialNum}"
    val confPath = s"./itest/testdata/gnode${serialNum}.conf"
    val gNodeLauncher = new GNodeLauncher(dbPath, confPath)
    gNodeLauncher.startServer()
    gNodeLauncher.registerAsReadNode()
  }

  def launchAsWriteNode(serialNum: Int): Unit = {
    val dbPath = s"./itest/output/testdb${serialNum}"
    val confPath = s"./itest/testdata/gnode${serialNum}.conf"
    val gNodeLauncher = new GNodeLauncher(dbPath, confPath)
    gNodeLauncher.startServer()
    gNodeLauncher.registerAsWriteNode()
  }

  //coor serialNum for test is 0
  def launchAsCoordinator(serialNum: Int): Unit ={
    val dbPath = s"./itest/output/testdb${serialNum}"
    val confPath = s"./testdata/gnode${serialNum}.conf"
    val gNodeLauncher = new CNodeLauncher(dbPath, confPath)
    gNodeLauncher.startServer()
  }
}
