package cn.pandadb.externalprops

import cn.pandadb.context.{InstanceBoundService, InstanceBoundServiceContext, InstanceBoundServiceFactory}

class CustomPropertyNodeStoreHolderFactory extends InstanceBoundServiceFactory {
  override def create(ctx: InstanceBoundServiceContext): Option[InstanceBoundService] = {
    val maybeFactoryClassName = ctx.instanceContext.getOption("external.properties.store.factory")
    maybeFactoryClassName.map(className => {
      val store = Class.forName(className).newInstance().asInstanceOf[ExternalPropertyStoreFactory].create(ctx)
      ctx.instanceContext.put[CustomPropertyNodeStore](store)
      store
    })
  }
}
