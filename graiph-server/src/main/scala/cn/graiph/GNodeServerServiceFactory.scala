package cn.graiph

import cn.graiph.db.{CustomDatabaseLifecycleService, CustomDatabaseLifecycleServiceContext, CustomDatabaseLifecycleServiceFactory}

/**
  * Created by bluejoe on 2019/11/7.
  */
class GNodeServerServiceFactory extends CustomDatabaseLifecycleServiceFactory {
  def create(ctx: CustomDatabaseLifecycleServiceContext): Option[CustomDatabaseLifecycleService] = {
    val zk = ctx.configuration.getRaw("...");
    //...
    ctx.instanceContext.put("", null);
    //ctx.instanceContext.put[CustomPropertyNodeStoreHolder]();
    None
  }
}

class CNodeServerServiceFactory extends CustomDatabaseLifecycleServiceFactory {
  def create(ctx: CustomDatabaseLifecycleServiceContext): Option[CustomDatabaseLifecycleService] = {
    None
  }
}
