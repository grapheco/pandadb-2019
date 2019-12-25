package cn.pandadb.externalprops

import cn.pandadb.context.InstanceBoundServiceFactoryRegistry
import cn.pandadb.util.{PandaModuleContext, PandaModule}

class ExternalPropetiesModule extends PandaModule {
  override def init(ctx: PandaModuleContext): Unit = {
    InstanceBoundServiceFactoryRegistry.register[CustomPropertyNodeStoreHolderFactory];
    //declare properties
  }

  override def stop(ctx: PandaModuleContext): Unit = {

  }

  override def start(ctx: PandaModuleContext): Unit = {

  }
}