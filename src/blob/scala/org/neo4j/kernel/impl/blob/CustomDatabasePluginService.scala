/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.blob

import java.io.File

import cn.graiph.util.{Configuration, ContextMap, Logging}
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.impl.Neo4jConfigUtils._
import org.neo4j.kernel.impl.factory.DatabaseInfo
import org.neo4j.kernel.impl.proc.Procedures
import org.neo4j.kernel.lifecycle.Lifecycle

import scala.collection.mutable.ArrayBuffer

trait CustomDatabasePlugin {
  def init(ctx: CustomDatabasePluginContext): Unit;

  def start(ctx: CustomDatabasePluginContext): Unit;

  def stop(ctx: CustomDatabasePluginContext): Unit;
}

object CustomDatabasePlugins extends Logging {
  val plugins = ArrayBuffer[CustomDatabasePlugin](
    new BlobStoragePlugin(),
    new DefaultBlobFunctionsPlugin()
  );

  def register(plugin: CustomDatabasePlugin) = plugins += plugin;

  def init(ctx: CustomDatabasePluginContext): Unit = {
    plugins.foreach { x =>
      x.init(ctx)
      logger.debug(s"plugin initialized: $x");
    }
  }

  def start(ctx: CustomDatabasePluginContext): Unit = {
    plugins.foreach {
      _.start(ctx)
    }
  }

  def stop(ctx: CustomDatabasePluginContext): Unit = {
    plugins.foreach {
      _.stop(ctx)
    }
  }
}

class BlobStoragePlugin extends CustomDatabasePlugin with Logging {
  var blobStorage: BlobStorage = _;

  override def init(ctx: CustomDatabasePluginContext): Unit = {
    blobStorage = BlobStorage.create(ctx.configuration);
    ctx.instanceContext.put[BlobStorage](blobStorage);
  }

  override def stop(ctx: CustomDatabasePluginContext): Unit = {
    blobStorage.disconnect();
    logger.info(s"blob storage disconnected: $blobStorage");
  }

  override def start(ctx: CustomDatabasePluginContext): Unit = {
    blobStorage.initialize(new File(ctx.storeDir,
      ctx.neo4jConf.get(GraphDatabaseSettings.active_database)),
      ctx.configuration);
  }
}

class DefaultBlobFunctionsPlugin extends CustomDatabasePlugin with Logging {
  override def init(ctx: CustomDatabasePluginContext): Unit = {
    registerProcedure(ctx.proceduresService, classOf[DefaultBlobFunctions]);
  }

  private def registerProcedure(proceduresService: Procedures, procedures: Class[_]*) {
    for (procedure <- procedures) {
      proceduresService.registerProcedure(procedure);
      proceduresService.registerFunction(procedure);
    }
  }

  override def stop(ctx: CustomDatabasePluginContext): Unit = {
  }

  override def start(ctx: CustomDatabasePluginContext): Unit = {
  }
}

case class CustomDatabasePluginContext(proceduresService: Procedures, storeDir: File, neo4jConf: Config, databaseInfo: DatabaseInfo, configuration: Configuration, instanceContext: ContextMap) {

}

class CustomDatabasePluginService(proceduresService: Procedures, storeDir: File, neo4jConf: Config, databaseInfo: DatabaseInfo)
  extends Lifecycle with Logging {

  val configuration: Configuration = neo4jConf;
  val ctx = new CustomDatabasePluginContext(proceduresService, storeDir, neo4jConf, databaseInfo, configuration, neo4jConf.getInstanceContext);

  /////binds context
  neo4jConf.getInstanceContext.put[CustomDatabasePluginService](this);

  override def shutdown(): Unit = {
  }

  override def init(): Unit = {
    CustomDatabasePlugins.init(ctx);
  }

  override def stop(): Unit = {
    CustomDatabasePlugins.stop(ctx);
  }

  override def start(): Unit = {
    CustomDatabasePlugins.start(ctx);
  }
}