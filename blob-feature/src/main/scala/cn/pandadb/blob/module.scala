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

import java.io.File

import cn.pandadb.context.{InstanceBoundService, InstanceBoundServiceContext, InstanceBoundServiceFactory, InstanceBoundServiceFactoryRegistry}
import cn.pandadb.util.ConfigUtils._
import cn.pandadb.util._
import org.neo4j.kernel.impl.blob.{BlobStorage, DefaultBlobFunctions}
import org.neo4j.kernel.impl.proc.Procedures

class BlobStorageModule extends PandaModule {
  override def init(ctx: PandaModuleContext): Unit = {
    InstanceBoundServiceFactoryRegistry.register[DefaultBlobFunctionsServiceFactory];

    val conf = ctx.configuration;
    val blobStorage = BlobStorage.create(conf);
    BlobStorageContext.bindBlobStorage(blobStorage);
    BlobStorageContext.bindBlobStorageDir(conf.getAsFile("blob.storage.file.dir", ctx.storeDir, new File(ctx.storeDir, "/blob")));
  }

  override def stop(ctx: PandaModuleContext): Unit = {

  }

  override def start(ctx: PandaModuleContext): Unit = {

  }
}

object BlobStorageContext extends ContextMap {
  def blobStorage: BlobStorage = get[BlobStorage]();

  def bindBlobStorage(blobStorage: BlobStorage): Unit = put[BlobStorage](blobStorage)

  def getDefaultBlobValueStorageClass: Option[String] = getOption("default-blob-value-storage-class")

  def bindBlobStorageDir(dir: File): Unit = put("blob.storage.file.dir", dir)

  def blobStorageDir: File = get("blob.storage.file.dir");
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