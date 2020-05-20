package cn.pandadb.cluster

import cn.pandadb.configuration.Config
import cn.pandadb.server.modules.{LifecycleServerModule}

class ClusterNode(config: Config) extends LifecycleServerModule {

  private val nodeServiceAddress = config.getNodeAddress()

  override def start(): Unit = {
  }

}
