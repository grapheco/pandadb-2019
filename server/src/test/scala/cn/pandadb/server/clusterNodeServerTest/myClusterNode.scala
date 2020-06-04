package cn.pandadb.server.clusterNodeServerTest

import cn.pandadb.cluster.ClusterService
import cn.pandadb.server.ClusterNodeServer
import cn.pandadb.store.local.DataStore
import org.apache.curator.shaded.com.google.common.net.HostAndPort

class myClusterNode(config: myConfig, clusterService: ClusterService, dataStore: myDataStore) extends
  ClusterNodeServer(config, clusterService, dataStore) {
  override def updateLocalDataToLatestVersion(): Unit = {
    dataStore.setDataVersion(clusterService.getDataVersion().toLong)
  }

  override def syncDataFromCluster(leaderNode: HostAndPort): Unit = {
    updateLocalDataToLatestVersion()
  }

}
