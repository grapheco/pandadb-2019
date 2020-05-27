package cn.pandadb.cluster
import java.util.concurrent.{ExecutorService, Executors}

import cn.pandadb.cluster.ClusterService
import cn.pandadb.configuration.Config
import cn.pandadb.zk.ZKTools
import org.apache.zookeeper.CreateMode
import org.junit.Test
// scalastyle:off println
class MyConfig extends Config {

  val zkAddress = "127.0.0.1:2181"
  var nodeAddress: String = null
  override def getZKAddress(): String = zkAddress

  override def getNodeAddress(): String = nodeAddress

}

class ThreadExample(id: Int, config: MyConfig) {

  var zktools: ZKTools = null
  var clusterService: ClusterService = null
  def run(): Unit = {
    if (id == 1) {
      config.nodeAddress = "192.168.1.1"
      zktools = new ZKTools(config)
      zktools.init()
      clusterService = new ClusterService(config, zktools)
      clusterService.localDataVersion = "1"
      clusterService.dataVersion = "15"

    }
    else {
      config.nodeAddress = "8.8.8.8"
      zktools = new ZKTools(config)
      zktools.init()
      clusterService = new ClusterService(config, zktools)
      clusterService.localDataVersion = "15"
      clusterService.dataVersion = "15"
    }
    clusterService.init()
    clusterService.doNodeStart2()
    while (zktools.getZKNodeChildren(clusterService.dataNodesPath).size < 1) {
      Thread.sleep(3000)
      println("I'm sleeping")
    }

  }

}

class ClusterTest {

  val config = new MyConfig
  val zktools = new ZKTools(config)

  @Test
  def singleNodeOnline(): Unit = {

    zktools.init()
    val clusterService = new ClusterService(config, zktools)
    clusterService.init()
    clusterService.doNodeStart2()
    println(clusterService.nodeAddress)
    println(clusterService.getLeaderNodeAddress())
    println(clusterService.getDataVersion())

  }

  @Test
  def multiNodeOnline(): Unit = {
    //val node1 = new ThreadExample(1, config)
    val node2 = new ThreadExample(3, config)
    node2.run()

  }

}
