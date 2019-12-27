package cn.pandadb.externalprops

import cn.pandadb.util._

class ExternalPropertiesModule extends PandaModule {
  override def init(ctx: PandaModuleContext): Unit = {
    val conf = ctx.configuration;
    val maybeFactoryClassName = conf.getRaw("external.properties.store.factory")

    maybeFactoryClassName.foreach(className => {
      val store = Class.forName(className).newInstance().asInstanceOf[ExternalPropertyStoreFactory].create(conf)
      ExternalPropertiesContext.put[CustomPropertyNodeStore](store);
    })

    import cn.pandadb.util.ConfigUtils._
    ExternalPropertiesContext.put("isExternalPropStorageEnabled", conf.getValueAsBoolean("external.property.storage.enabled", false))
  }

  override def stop(ctx: PandaModuleContext): Unit = {

  }

  override def start(ctx: PandaModuleContext): Unit = {

  }
}

object ExternalPropertiesContext extends ContextMap {
  def customPropertyNodeStore: CustomPropertyNodeStore = get[CustomPropertyNodeStore]();

  def isExternalPropStorageEnabled: Boolean = super.get("isExternalPropStorageEnabled")
}