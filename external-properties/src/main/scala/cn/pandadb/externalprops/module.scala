package cn.pandadb.externalprops

import cn.pandadb.util._

class ExternalPropertiesModule extends PandaModule {
  override def init(ctx: PandaModuleContext): Unit = {
    val conf = ctx.configuration;
    import cn.pandadb.util.ConfigUtils._

    val isExternalPropertyStorageEnabled = conf.getValueAsBoolean("external.property.storage.enabled", false)
    if (isExternalPropertyStorageEnabled) {
      val factoryClassName = conf.getRequiredValueAsString("external.properties.store.factory")

      val store = Class.forName(factoryClassName).newInstance().asInstanceOf[ExternalPropertyStoreFactory].create(conf)
      ExternalPropertiesContext.bindCustomPropertyNodeStore(store);
    }
  }

  override def stop(ctx: PandaModuleContext): Unit = {

  }

  override def start(ctx: PandaModuleContext): Unit = {

  }
}

object ExternalPropertiesContext extends ContextMap {
  def maybeCustomPropertyNodeStore: Option[CustomPropertyNodeStore] = getOption[CustomPropertyNodeStore]

  def bindCustomPropertyNodeStore(store: CustomPropertyNodeStore): Unit = put[CustomPropertyNodeStore](store);

  def isExternalPropStorageEnabled: Boolean = maybeCustomPropertyNodeStore.isDefined
}