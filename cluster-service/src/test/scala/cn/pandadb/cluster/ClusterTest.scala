package cn.pandadb.cluster
import cn.pandadb.cluster.ClusterService
import cn.pandadb.configuration.Config
import cn.pandadb.zk.ZKTools
import org.apache.zookeeper.CreateMode
import org.junit.Test
// scalastyle:off println
class MyConfig extends Config {

  override def getZKAddress(): String = "127.0.0.1:2181"

  override def getNodeAddress(): String = "192.168.1.5"

}

class ZKToolsTest {
  val config = new MyConfig
  val zkTools = new ZKTools(config)

  val nodeAddress = config.getNodeAddress()

  val leaderNodesPath = zkTools.buildFullZKPath("/leaderNodes")
  val dataNodesPath = zkTools.buildFullZKPath("/dataNodes")
  val dataVersionPath = zkTools.buildFullZKPath("/dataVersion")
  val freshNodesPath = zkTools.buildFullZKPath("/freshNodes")
  val versionZero = "0"

  @Test
  def testAssurePathExist(): Unit = {

    zkTools.init()
    zkTools.assureZKNodeExist(leaderNodesPath, dataVersionPath, dataNodesPath, freshNodesPath)
    //val data = zkTools.getZKNodeData(dataVersionPath)
/*    println(zkTools.getZKNodeData(dataVersionPath))
    println(zkTools.getZKNodeData(leaderNodesPath))
    println(zkTools.getZKNodeData(dataNodesPath))
    println(zkTools.getZKNodeData(freshNodesPath))*/
  }

  @Test
  def testCreateAndGetData(): Unit = {
    val path = "/test/hiha"
    zkTools.init()
    zkTools.createZKNode(CreateMode.PERSISTENT, path)
    zkTools.createZKNode(CreateMode.PERSISTENT, path + "/jkl")
    println(zkTools.getZKNodeChildren("/test/hiha"))
    zkTools.deleteZKNodeAndChildren("/test")
  }

}

class ClusterTest {

  val config = new MyConfig
  val zktools = new ZKTools(config)

  @Test
  def nodeOnline(): Unit = {

    zktools.init()
    val clusterService = new ClusterService(config, zktools)
    clusterService.init()
    clusterService.doNodeStart2()
    println(clusterService.nodeAddress)
    println(clusterService.getLeaderNodeAddress())
    println(clusterService.getDataVersion())

  }

}
