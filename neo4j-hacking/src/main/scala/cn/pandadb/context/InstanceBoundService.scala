package cn.pandadb.context

import java.io.File

import cn.pandadb.context.Neo4jConfigUtils._
import cn.pandadb.util.{Configuration, ContextMap, Logging}
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.impl.factory.DatabaseInfo
import org.neo4j.kernel.impl.proc.Procedures
import org.neo4j.kernel.lifecycle.Lifecycle

import scala.collection.mutable.ArrayBuffer

trait InstanceBoundServiceFactory {
  def create(ctx: InstanceBoundServiceContext): Option[InstanceBoundService];
}

case class InstanceBoundServiceContext(proceduresService: Procedures, storeDir: File, neo4jConf: Config, databaseInfo: DatabaseInfo, configuration: Configuration, instanceContext: ContextMap) {

}

/**
  * CustomDatabaseLifecyclePlugins records all plugins which will be maintained/active in whole life cycle of current database instance
  */
object InstanceBoundServiceFactoryRegistry extends Logging {
  val plugins = ArrayBuffer[InstanceBoundServiceFactory]();
  val services = ArrayBuffer[InstanceBoundService]();

  def register[T <: InstanceBoundServiceFactory](implicit manifest: Manifest[T]): InstanceBoundServiceFactory = {
    register(manifest.runtimeClass.newInstance().asInstanceOf[InstanceBoundServiceFactory])
  }

  def register(plugin: InstanceBoundServiceFactory): InstanceBoundServiceFactory = {
    if (plugins.exists(_.getClass == plugin.getClass))
      throw new RuntimeException(s"duplicate plugin: ${plugin.getClass}")

    plugins += plugin
    plugin
  }

  def init(ctx: InstanceBoundServiceContext): Unit = {
    services ++= plugins.flatMap { f =>
      logger.debug(s"plugin initialized: $f");
      f.create(ctx)
    }
  }

  def start(ctx: InstanceBoundServiceContext): Unit = {
    services.foreach(_.start(ctx))
  }

  def stop(ctx: InstanceBoundServiceContext): Unit = {
    services.foreach(_.stop(ctx))
  }
}

class InstanceBoundServiceFactoryRegistryHolder(proceduresService: Procedures, storeDir: File, neo4jConf: Config, databaseInfo: DatabaseInfo)
  extends Lifecycle with Logging {

  val configuration: Configuration = neo4jConf;
  val ctx = new InstanceBoundServiceContext(proceduresService, storeDir, neo4jConf, databaseInfo, configuration, neo4jConf.getInstanceContext);

  /////binds context
  neo4jConf.getInstanceContext.put[InstanceBoundServiceFactoryRegistryHolder](this);

  override def shutdown(): Unit = {
  }

  override def init(): Unit = {
    InstanceBoundServiceFactoryRegistry.init(ctx);
  }

  override def stop(): Unit = {
    InstanceBoundServiceFactoryRegistry.stop(ctx);
  }

  override def start(): Unit = {
    InstanceBoundServiceFactoryRegistry.start(ctx);
  }
}

trait InstanceBoundService {
  def start(ctx: InstanceBoundServiceContext): Unit;

  def stop(ctx: InstanceBoundServiceContext): Unit;
}
