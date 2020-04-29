package cn.pandadb.server

import cn.pandadb.configuration.Config
import cn.pandadb.lifecycle.LifecycleSupport
import cn.pandadb.costore.CostoreServer
import cn.pandadb.cluster.ClusterService
import cn.pandadb.datanode.PandaRpcServer

class PandaServer(config: Config)  {

  val life = new LifecycleSupport
  val logger = config.getLogger(this.getClass)
  life.add(new ClusterService(config))
  life.add(new CostoreServer(config) )
  life.add(new PandaRpcServer(config) )

  def start(): Unit = {
    logger.info("==== PandaDB Server Start... ====")
//    driverServer.start(config)
    life.start()
  }

  def stop(): Unit = {
    logger.info("==== PandaDB Server Stop... ====")
//    driverServer.stop()
    life.stop()
  }
}
