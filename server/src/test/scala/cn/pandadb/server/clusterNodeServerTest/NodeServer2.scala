package cn.pandadb.server.clusterNodeServerTest

import cn.pandadb.cluster.ClusterService
import cn.pandadb.zk.ZKTools

//scalastyle:off println

object NodeServer2 {

  def main(args: Array[String]): Unit = {
    val config1 = new myConfig()
    config1.nodeAddress = "8.8.8.8:1001"
    config1.zkAddress = "127.0.0.1:2181"

    val zktools = new ZKTools(config1)
    zktools.init()
    val clusterService = new ClusterService(config1, zktools) {
      override def getDataVersion(): String = {
        dataVersion
      }
    }
    clusterService.dataVersion = "10"
    clusterService.init()
    val dataStore = new myDataStore()
    dataStore.dataVersion = "10"
    val nodeServer1 = new myClusterNode(config1, clusterService, dataStore)
    nodeServer1.start()

    while (true) {
      println(nodeServer1.nodeHostAndPort + ": I'm sleep")
      Thread.sleep(3000)
    }
  }

}
