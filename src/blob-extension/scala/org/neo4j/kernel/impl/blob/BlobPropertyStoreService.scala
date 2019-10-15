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

import org.neo4j.blob.utils.{Configuration, ContextMap, Logging}
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.impl.Neo4jConfigUtils._
import org.neo4j.kernel.impl.factory.DatabaseInfo
import org.neo4j.kernel.impl.proc.Procedures
import org.neo4j.kernel.lifecycle.Lifecycle

import scala.collection.mutable.ArrayBuffer

trait BlobPropertyStoreServicePlugin {
  def init(ctx: BlobPropertyStoreServiceContext): Unit;

  def start(ctx: BlobPropertyStoreServiceContext): Unit;

  def stop(ctx: BlobPropertyStoreServiceContext): Unit;
}

object BlobPropertyStoreServicePlugins extends Logging {
  val plugins = ArrayBuffer[BlobPropertyStoreServicePlugin](
    new BlobStoragePlugin(),
    new DefaultBlobFunctionsPlugin()
  );

  def add(plugin: BlobPropertyStoreServicePlugin) = plugins += plugin;

  def init(ctx: BlobPropertyStoreServiceContext): Unit = {
    plugins.foreach { x =>
      x.init(ctx)
      logger.debug(s"plugin initialized: $x");
    }
  }

  def start(ctx: BlobPropertyStoreServiceContext): Unit = {
    plugins.foreach {
      _.start(ctx)
    }
  }

  def stop(ctx: BlobPropertyStoreServiceContext): Unit = {
    plugins.foreach {
      _.stop(ctx)
    }
  }
}

class BlobStoragePlugin extends BlobPropertyStoreServicePlugin with Logging {
  var blobStorage: BlobStorage = _;

  override def init(ctx: BlobPropertyStoreServiceContext): Unit = {
    blobStorage = BlobStorage.create(ctx.configuration);
    ctx.instanceContext.put[BlobStorage](blobStorage);
  }

  override def stop(ctx: BlobPropertyStoreServiceContext): Unit = {
    blobStorage.disconnect();
    logger.info(s"blob storage disconnected: $blobStorage");
  }

  override def start(ctx: BlobPropertyStoreServiceContext): Unit = {
    blobStorage.initialize(new File(ctx.storeDir,
      ctx.neo4jConf.get(GraphDatabaseSettings.active_database)),
      ctx.configuration);
  }
}

class DefaultBlobFunctionsPlugin extends BlobPropertyStoreServicePlugin with Logging {
  override def init(ctx: BlobPropertyStoreServiceContext): Unit = {
    registerProcedure(ctx.proceduresService, classOf[DefaultBlobFunctions]);
  }

  private def registerProcedure(proceduresService: Procedures, procedures: Class[_]*) {
    for (procedure <- procedures) {
      proceduresService.registerProcedure(procedure);
      proceduresService.registerFunction(procedure);
    }
  }

  override def stop(ctx: BlobPropertyStoreServiceContext): Unit = {
  }

  override def start(ctx: BlobPropertyStoreServiceContext): Unit = {
  }
}

case class BlobPropertyStoreServiceContext(proceduresService: Procedures, storeDir: File, neo4jConf: Config, databaseInfo: DatabaseInfo, configuration: Configuration, instanceContext: ContextMap) {

}

class BlobPropertyStoreService(proceduresService: Procedures, storeDir: File, neo4jConf: Config, databaseInfo: DatabaseInfo)
  extends Lifecycle with Logging {

  val configuration: Configuration = neo4jConf;
  val ctx = new BlobPropertyStoreServiceContext(proceduresService, storeDir, neo4jConf, databaseInfo, configuration, neo4jConf.getInstanceContext);

  /////binds context
  neo4jConf.getInstanceContext.put[BlobPropertyStoreService](this);

  override def shutdown(): Unit = {
  }

  override def init(): Unit = {
    BlobPropertyStoreServicePlugins.init(ctx);
  }

  override def stop(): Unit = {
    BlobPropertyStoreServicePlugins.stop(ctx);
  }

  override def start(): Unit = {
    BlobPropertyStoreServicePlugins.start(ctx);
  }
}