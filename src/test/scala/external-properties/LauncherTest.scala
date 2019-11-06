import java.io.FileInputStream
import java.net.InetAddress
import java.util.Properties

import cn.graiph.cnode.GNodeLauncher
import org.junit.{Assert, Test}

object LauncherTest {

  def main(args: Array[String]): Unit = {
    // change the serialNum from 1 to 2 in different IDEA
    //launchAsReadNode(1)

    // change the serialNum from 3 to 4 in different IDEA
    launchAsWriteNode(3)
  }

  // for easy test only: valid serialNum value is 1,2,3,4
  def testGNodeLauncher(serialNum: Int): Unit ={

    val dbPath = s"./output/testdb${serialNum}"
    val confPath = s"./testdata/gnode${serialNum}.conf"
    val gNodeLauncher = new GNodeLauncher(dbPath, confPath)
    gNodeLauncher.startServer()
  }

  def launchAsReadNode(serialNum: Int): Unit = {
    val dbPath = s"./output/testdb${serialNum}"
    val confPath = s"./testdata/gnode${serialNum}.conf"
    val gNodeLauncher = new GNodeLauncher(dbPath, confPath)
    gNodeLauncher.startServer()
    gNodeLauncher.registerAsReadNode()
  }

  def launchAsWriteNode(serialNum: Int): Unit = {
    val dbPath = s"./output/testdb${serialNum}"
    val confPath = s"./testdata/gnode${serialNum}.conf"
    val gNodeLauncher = new GNodeLauncher(dbPath, confPath)
    gNodeLauncher.startServer()
    gNodeLauncher.registerAsWriteNode()
  }

  //coor serialNum for test is 0
  def launchAsCoordinator(serialNum: Int): Unit ={
    val dbPath = s"./output/testdb${serialNum}"
    val confPath = s"./testdata/gnode${serialNum}.conf"
    val gNodeLauncher = new GNodeLauncher(dbPath, confPath)
    gNodeLauncher.startServer()
  }
}
