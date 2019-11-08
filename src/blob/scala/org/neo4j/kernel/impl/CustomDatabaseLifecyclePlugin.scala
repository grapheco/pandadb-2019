package org.neo4j.kernel.impl

import java.io.File

import cn.graiph.util.{ContextMap, Configuration, Logging}
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.impl.blob.{BlobStoragePlugin, DefaultBlobFunctionsPlugin}
import org.neo4j.kernel.impl.factory.DatabaseInfo
import org.neo4j.kernel.impl.proc.Procedures
import org.neo4j.kernel.lifecycle.Lifecycle
import org.neo4j.kernel.impl.Neo4jConfigUtils._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bluejoe on 2019/11/7.
  */
trait CustomDatabaseLifecyclePlugin {
  def init(ctx: CustomDatabaseLifecyclePluginContext): Unit;

  def start(ctx: CustomDatabaseLifecyclePluginContext): Unit;

  def stop(ctx: CustomDatabaseLifecyclePluginContext): Unit;
}

case class CustomDatabaseLifecyclePluginContext(proceduresService: Procedures, storeDir: File, neo4jConf: Config, databaseInfo: DatabaseInfo, configuration: Configuration, instanceContext: ContextMap) {

}

/**
  * CustomDatabaseLifecyclePlugins records all plugins which will be maintained/active in whole life cycle of current database instance
  */
object CustomDatabaseLifecyclePlugins extends Logging {
  val plugins = ArrayBuffer[CustomDatabaseLifecyclePlugin](
    new BlobStoragePlugin(),
    new DefaultBlobFunctionsPlugin()
  );

  def register[T <: CustomDatabaseLifecyclePlugin](implicit manifest: Manifest[T]): CustomDatabaseLifecyclePlugin = {
    register(manifest.runtimeClass.newInstance().asInstanceOf[CustomDatabaseLifecyclePlugin])
  }

  def register(plugin: CustomDatabaseLifecyclePlugin): CustomDatabaseLifecyclePlugin = {
    if (plugins.exists(_.getClass == plugin.getClass))
      throw new RuntimeException(s"duplicate plugin: ${plugin.getClass}")

    plugins += plugin
    plugin
  }

  def init(ctx: CustomDatabaseLifecyclePluginContext): Unit = {
    plugins.foreach { x =>
      x.init(ctx)
      logger.debug(s"plugin initialized: $x");
    }
  }

  def start(ctx: CustomDatabaseLifecyclePluginContext): Unit = {
    plugins.foreach {
      _.start(ctx)
    }
  }

  def stop(ctx: CustomDatabaseLifecyclePluginContext): Unit = {
    plugins.foreach {
      _.stop(ctx)
    }
  }
}

class CustomDatabaseLifecyclePluginService(proceduresService: Procedures, storeDir: File, neo4jConf: Config, databaseInfo: DatabaseInfo)
  extends Lifecycle with Logging {

  val configuration: Configuration = neo4jConf;
  val ctx = new CustomDatabaseLifecyclePluginContext(proceduresService, storeDir, neo4jConf, databaseInfo, configuration, neo4jConf.getInstanceContext);

  /////binds context
  neo4jConf.getInstanceContext.put[CustomDatabaseLifecyclePluginService](this);

  override def shutdown(): Unit = {
  }

  override def init(): Unit = {
    CustomDatabaseLifecyclePlugins.init(ctx);
  }

  override def stop(): Unit = {
    CustomDatabaseLifecyclePlugins.stop(ctx);
  }

  override def start(): Unit = {
    CustomDatabaseLifecyclePlugins.start(ctx);
  }
}