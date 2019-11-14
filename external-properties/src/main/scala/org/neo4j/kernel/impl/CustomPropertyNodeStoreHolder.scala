package org.neo4j.kernel.impl

import cn.graiph.context.{InstanceBoundService, InstanceBoundServiceContext, InstanceBoundServiceFactory}

/**
  * Created by bluejoe on 2019/10/7.
  */
object Settings {
  var _hookEnabled = false;
  var _patternMatchFirst = true;
}

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
