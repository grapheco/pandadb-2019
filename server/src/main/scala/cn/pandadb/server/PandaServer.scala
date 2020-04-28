package cn.pandadb.server

import cn.pandadb.configuration.Config
import cn.pandadb.lifecycle.LifecycleSupport
import cn.pandadb.costore.CostoreServer
//import cn.pandadb.neo4j.Neo4jServer
import cn.pandadb.datanode.DataNodeRpcServer

class PandaServer(config: Config)  {

  val life = new LifecycleSupport
  val logger = config.getLogger(this.getClass)
  life.add(new CostoreServer(config) )
  life.add(new DataNodeRpcServer(config) )

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
