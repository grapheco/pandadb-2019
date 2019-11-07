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
import org.neo4j.kernel.impl.{CustomDatabaseLifecyclePluginContext, CustomDatabaseLifecyclePlugin}
import org.neo4j.kernel.impl.proc.Procedures

class BlobStoragePlugin extends CustomDatabaseLifecyclePlugin with Logging {
  var blobStorage: BlobStorage = _;

  override def init(ctx: CustomDatabaseLifecyclePluginContext): Unit = {
    blobStorage = BlobStorage.create(ctx.configuration);
    ctx.instanceContext.put[BlobStorage](blobStorage);
  }

  override def stop(ctx: CustomDatabaseLifecyclePluginContext): Unit = {
    blobStorage.disconnect();
    logger.info(s"blob storage disconnected: $blobStorage");
  }

  override def start(ctx: CustomDatabaseLifecyclePluginContext): Unit = {
    blobStorage.initialize(new File(ctx.storeDir,
      ctx.neo4jConf.get(GraphDatabaseSettings.active_database)),
      ctx.configuration);
  }
}

class DefaultBlobFunctionsPlugin extends CustomDatabaseLifecyclePlugin with Logging {
  override def init(ctx: CustomDatabaseLifecyclePluginContext): Unit = {
    registerProcedure(ctx.proceduresService, classOf[DefaultBlobFunctions]);
  }

  private def registerProcedure(proceduresService: Procedures, procedures: Class[_]*) {
    for (procedure <- procedures) {
      proceduresService.registerProcedure(procedure);
      proceduresService.registerFunction(procedure);
    }
  }

  override def stop(ctx: CustomDatabaseLifecyclePluginContext): Unit = {
  }

  override def start(ctx: CustomDatabaseLifecyclePluginContext): Unit = {
  }
}