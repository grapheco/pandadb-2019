package cn.pandadb.util

import scala.collection.mutable.ArrayBuffer

trait PandaModule {
  def init(ctx: PandaModuleContext);

  def start(ctx: PandaModuleContext);

  def stop(ctx: PandaModuleContext);
}

case class PandaModuleContext(instanceContext: ContextMap, config: PropertyRegistry) {
  def declareParameter(parser: PropertyParser): Unit = config.register(parser);
}

class PandaModules extends Logging {
  val modules = ArrayBuffer[PandaModule]();

  def add(module: PandaModule): Unit = modules += module;

  def init(ctx: PandaModuleContext): Unit = modules.foreach { module =>
    module.init(ctx)
    logger.info(s"initialized ${module.getClass.getSimpleName}")
  }

  def start(ctx: PandaModuleContext): Unit = modules.foreach(_.start(ctx))

  def stop(ctx: PandaModuleContext): Unit = modules.foreach(_.stop(ctx))
}