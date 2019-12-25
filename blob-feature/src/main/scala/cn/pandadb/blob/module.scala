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
package cn.pandadb.blob

import cn.pandadb.context.{InstanceBoundServiceFactoryRegistry, InstanceBoundService, InstanceBoundServiceContext, InstanceBoundServiceFactory}
import cn.pandadb.cypherplus.SemanticOperatorServiceFactory
import cn.pandadb.util.{PandaModuleContext, PandaModule, Logging}
import org.neo4j.kernel.impl.blob.{DefaultBlobFunctions, BlobStorage}
import org.neo4j.kernel.impl.proc.Procedures

class BlobStorageModule extends PandaModule {
  override def init(ctx: PandaModuleContext): Unit = {
    InstanceBoundServiceFactoryRegistry.register[BlobStorageServiceFactory];
    InstanceBoundServiceFactoryRegistry.register[DefaultBlobFunctionsServiceFactory];
    InstanceBoundServiceFactoryRegistry.register[SemanticOperatorServiceFactory];

    //declare properties
  }

  override def stop(ctx: PandaModuleContext): Unit = {

  }

  override def start(ctx: PandaModuleContext): Unit = {

  }
}

class BlobStorageServiceFactory extends InstanceBoundServiceFactory with Logging {
  override def create(ctx: InstanceBoundServiceContext): Option[InstanceBoundService] = {
    val blobStorage = BlobStorage.create(ctx.instanceContext);
    ctx.instanceContext.put[BlobStorage](blobStorage);
    Some(blobStorage)
  }
}

class DefaultBlobFunctionsServiceFactory extends InstanceBoundServiceFactory with Logging {
  override def create(ctx: InstanceBoundServiceContext): Option[InstanceBoundService] = {
    registerProcedure(ctx.proceduresService, classOf[DefaultBlobFunctions]);
    None
  }

  private def registerProcedure(proceduresService: Procedures, procedures: Class[_]*) {
    for (procedure <- procedures) {
      proceduresService.registerProcedure(procedure);
      proceduresService.registerFunction(procedure);
    }
  }
}