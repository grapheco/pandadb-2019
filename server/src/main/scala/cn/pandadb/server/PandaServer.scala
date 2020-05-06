package cn.pandadb.server

import cn.pandadb.configuration.Config
import cn.pandadb.lifecycle.LifecycleSupport
import cn.pandadb.costore.CostoreServer
import cn.pandadb.cluster.ClusterService
import cn.pandadb.datanode.PandaRpcServer
import cn.pandadb.zk.ZKTools

class PandaServer(config: Config)  {

  val life = new LifecycleSupport
  val logger = config.getLogger(this.getClass)
  val zkTools = new ZKTools(config)
  zkTools.init()

  life.add(new ClusterService(config, zkTools))
  life.add(new CostoreServer(config) )
  life.add(new PandaRpcServer(config) )

  def start(): Unit = {
    logger.info("==== PandaDB Server Starting... ====")
    life.start()
    logger.info("==== PandaDB Server is Started ====")
  }

  def shutdown(): Unit = {
    logger.info("==== PandaDB Server Shutting Down... ====")
    life.shutdown()
    zkTools.close()
    logger.info("==== PandaDB Server is Shutdown ====")
  }
}
