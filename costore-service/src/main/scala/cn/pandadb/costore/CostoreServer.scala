package cn.pandadb.costore

import cn.pandadb.configuration.Config
import cn.pandadb.server.modules.LifecycleServerModule

class CostoreServer(config: Config) extends LifecycleServerModule {
  val logger = config.getLogger(this.getClass)
  override def init(): Unit = {
    logger.info(this.getClass + ": init")
  }

  override def start(): Unit = {
    logger.info(this.getClass + ": start")
  }

  override def stop(): Unit = {
    logger.info(this.getClass + ": stop")
  }

  override def shutdown(): Unit = {
    logger.info(this.getClass + ": stop")
  }
}
