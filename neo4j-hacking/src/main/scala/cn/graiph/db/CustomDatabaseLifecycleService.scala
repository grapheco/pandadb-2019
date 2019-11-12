package cn.graiph.db

import java.io.File

import cn.graiph.util.{Configuration, ContextMap, Logging}
import org.neo4j.kernel.configuration.Config
import Neo4jConfigUtils._
import org.neo4j.kernel.impl.factory.DatabaseInfo
import org.neo4j.kernel.impl.proc.Procedures
import org.neo4j.kernel.lifecycle.Lifecycle

import scala.collection.mutable.ArrayBuffer

trait CustomDatabaseLifecycleService {
  def start(ctx: CustomDatabaseLifecycleServiceContext): Unit;

  def stop(ctx: CustomDatabaseLifecycleServiceContext): Unit;
}

trait CustomDatabaseLifecycleServiceFactory {
  def create(ctx: CustomDatabaseLifecycleServiceContext): Option[CustomDatabaseLifecycleService];
}

case class CustomDatabaseLifecycleServiceContext(proceduresService: Procedures, storeDir: File, neo4jConf: Config, databaseInfo: DatabaseInfo, configuration: Configuration, instanceContext: ContextMap) {

}

/**
  * CustomDatabaseLifecyclePlugins records all plugins which will be maintained/active in whole life cycle of current database instance
  */
object CustomDatabaseLifecycleServiceFactoryRegistry extends Logging {
  val plugins = ArrayBuffer[CustomDatabaseLifecycleServiceFactory]();
  val services = ArrayBuffer[CustomDatabaseLifecycleService]();

  def register[T <: CustomDatabaseLifecycleServiceFactory](implicit manifest: Manifest[T]): CustomDatabaseLifecycleServiceFactory = {
    register(manifest.runtimeClass.newInstance().asInstanceOf[CustomDatabaseLifecycleServiceFactory])
  }

  def register(plugin: CustomDatabaseLifecycleServiceFactory): CustomDatabaseLifecycleServiceFactory = {
    if (plugins.exists(_.getClass == plugin.getClass))
      throw new RuntimeException(s"duplicate plugin: ${plugin.getClass}")

    plugins += plugin
    plugin
  }

  def init(ctx: CustomDatabaseLifecycleServiceContext): Unit = {
    services ++= plugins.flatMap { f =>
      logger.debug(s"plugin initialized: $f");
      f.create(ctx)
    }
  }

  def start(ctx: CustomDatabaseLifecycleServiceContext): Unit = {
    services.foreach(_.start(ctx))
  }

  def stop(ctx: CustomDatabaseLifecycleServiceContext): Unit = {
    services.foreach(_.stop(ctx))
  }
}

class CustomDatabaseLifecycleServiceFactoryRegistryStarter(proceduresService: Procedures, storeDir: File, neo4jConf: Config, databaseInfo: DatabaseInfo)
  extends Lifecycle with Logging {

  val configuration: Configuration = neo4jConf;
  val ctx = new CustomDatabaseLifecycleServiceContext(proceduresService, storeDir, neo4jConf, databaseInfo, configuration, neo4jConf.getInstanceContext);

  /////binds context
  neo4jConf.getInstanceContext.put[CustomDatabaseLifecycleServiceFactoryRegistryStarter](this);

  override def shutdown(): Unit = {
  }

  override def init(): Unit = {
    CustomDatabaseLifecycleServiceFactoryRegistry.init(ctx);
  }

  override def stop(): Unit = {
    CustomDatabaseLifecycleServiceFactoryRegistry.stop(ctx);
  }

  override def start(): Unit = {
    CustomDatabaseLifecycleServiceFactoryRegistry.start(ctx);
  }
}