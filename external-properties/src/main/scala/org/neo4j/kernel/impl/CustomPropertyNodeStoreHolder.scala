package org.neo4j.kernel.impl

import cn.pandadb.context.{InstanceBoundService, InstanceBoundServiceContext, InstanceBoundServiceFactory}

class CustomPropertyNodeStoreHolderFactory extends InstanceBoundServiceFactory {
  override def create(ctx: InstanceBoundServiceContext): Option[InstanceBoundService] = {
    val maybeFactoryClassName = ctx.configuration.getRaw("external.properties.store.factory")
    maybeFactoryClassName.map(className => {
      val store = Class.forName(className).newInstance().asInstanceOf[PropertyStoreFactory].create(ctx)
      ctx.instanceContext.put[CustomPropertyNodeStore](store)
      store
    })
  }
}
