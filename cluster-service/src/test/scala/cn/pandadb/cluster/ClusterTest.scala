package cn.pandadb.cluster
import java.util.concurrent.{ExecutorService, Executors}

import cn.pandadb.cluster.ClusterService
import cn.pandadb.configuration.Config
import cn.pandadb.zk.ZKTools
import org.apache.zookeeper.CreateMode
import org.junit.Test
// scalastyle:off println

class ClusterTest {
  val config = new Config()
  val clusterService = new ClusterService(config)

  @Test
  def testForGetDataNodes(): Unit = {
    println(clusterService.getDataNodes())
  }
}
