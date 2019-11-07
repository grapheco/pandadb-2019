package cn.graiph

import org.neo4j.kernel.impl.{CustomDatabaseLifecyclePluginContext, CustomDatabaseLifecyclePlugin}

/**
  * Created by bluejoe on 2019/11/7.
  */
class GNodeServerPlugin extends CustomDatabaseLifecyclePlugin{
  override def init(ctx: CustomDatabaseLifecyclePluginContext): Unit = {
    val zk = ctx.configuration.getRaw("...");
    //...
    ctx.instanceContext.put("", null);
    //ctx.instanceContext.put[CustomPropertyNodeStoreHolder]();
  }

  override def stop(ctx: CustomDatabaseLifecyclePluginContext): Unit = {

  }

  override def start(ctx: CustomDatabaseLifecyclePluginContext): Unit = {

  }
}

class CNodeServerPlugin extends CustomDatabaseLifecyclePlugin{
  override def init(ctx: CustomDatabaseLifecyclePluginContext): Unit = {

  }

  override def stop(ctx: CustomDatabaseLifecyclePluginContext): Unit = {

  }

  override def start(ctx: CustomDatabaseLifecyclePluginContext): Unit = {

  }
}
