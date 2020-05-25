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

class ClusterTest {

  val config = new MyConfig
  val zktools = new ZKTools(config)
  println(config.getZKAddress())
  println(config.getPandaZKDir())
  println(config.getNodeAddress())

  @Test
  def nodeOnline(): Unit = {

    zktools.init()
    val clusterService = new ClusterService(config, zktools)
    clusterService.init()
    clusterService.doNodeStart()

  }

}
