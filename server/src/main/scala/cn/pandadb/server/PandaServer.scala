package cn.pandadb.server

import cn.pandadb.server.Config
import cn.pandadb.server.driver.{Server => DriverServer}
import cn.pandadb.server.util.Logging

class PandaServer(config: Config) extends Logging{

  val modules = new PandaModules
  val driverServer: DriverServer = new DriverServer

  modules.add(driverServer)

  def start(): Unit = {
    logger.info("==== PandaDB Server Start... ====")
//    driverServer.start(config)
    modules.start(config)
  }

  def stop(): Unit = {
    logger.info("==== PandaDB Server Stop... ====")
//    driverServer.stop()
    modules.stop(config)
  }
}
