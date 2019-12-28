package cn.pandadb.util

import java.io.File

import scala.collection.mutable.ArrayBuffer

trait ClosableModuleComponent {
  def start(ctx: PandaModuleContext): Unit

  def close(ctx: PandaModuleContext): Unit
}

trait PandaModule extends ClosableModuleComponent {
  def init(ctx: PandaModuleContext);
}

case class PandaModuleContext(configuration: Configuration, storeDir: File, sharedContext: ContextMap) {
}

class PandaModules extends Logging {
  val modules = ArrayBuffer[PandaModule]();

  def add(module: PandaModule): PandaModules = {
    modules += module
    this
  }

  def init(ctx: PandaModuleContext): Unit = modules.foreach { module =>
    module.init(ctx)
    logger.info(s"initialized ${module.getClass.getSimpleName}")
  }

  def start(ctx: PandaModuleContext): Unit = modules.foreach(_.start(ctx))

  def close(ctx: PandaModuleContext): Unit = modules.foreach(_.close(ctx))
}