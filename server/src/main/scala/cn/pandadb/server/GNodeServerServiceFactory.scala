package cn.pandadb.server

import cn.pandadb.context.InstanceBoundService
import cn.pandadb.context.{InstanceBoundServiceContext, InstanceBoundServiceFactory}

/**
  * Created by bluejoe on 2019/11/7.
  */
class GNodeServerServiceFactory extends InstanceBoundServiceFactory {
  def create(ctx: InstanceBoundServiceContext): Option[InstanceBoundService] = {
    val zk = ctx.configuration.getRaw("...");
    //...
    ctx.instanceContext.put("", null);
    //ctx.instanceContext.put[CustomPropertyNodeStoreHolder]();
    None
  }
}
