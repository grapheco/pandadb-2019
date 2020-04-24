package cn.pandadb.server

import cn.pandadb.server.Config
import cn.pandadb.server.util.Logging
import scala.collection.mutable.ArrayBuffer


trait ClosableModuleComponent {
  def start(config: Config): Unit

  def stop(): Unit
}

trait PandaModule extends ClosableModuleComponent {
  def init(config: Config): Unit
}

class PandaModules extends Logging {
  val modules = ArrayBuffer[PandaModule]();

  def add(module: PandaModule): PandaModules = {
    modules += module
    this
  }

  def init(config: Config): Unit = modules.foreach { module =>
    module.init(config)
    logger.info(s"initialized ${module.getClass.getSimpleName}")
  }

  def start(config: Config): Unit = modules.foreach(_.start(config))

  def stop(config: Config): Unit = modules.foreach(_.stop())
}