package cn.pandadb.server

import java.io.File
import cn.pandadb.configuration.Config

class PandaServerBootstrapper extends Bootstrapper {
  private var shutdownHook = null
  private var pandaServer: PandaServer = null
  def start(configFile: Option[File] = None, configOverrides: Map[String, String] = null): Unit = {
    addShutdownHook()
    val config = new Config().withFile(configFile).withSettings(configOverrides)
    pandaServer = new PandaServer(config)
    pandaServer.start()
  }

  private def addShutdownHook(): Unit = {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      override def run(): Unit = {
        doShutDown()
      }
    })
  }

  def doShutDown(): Unit = {
    stop()
  }

  def stop(): Unit = {
    pandaServer.stop()
  }

}
