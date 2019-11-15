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

package cn.graiph.context

import cn.graiph.util.ReflectUtils._
import cn.graiph.util.{ConfigUtils, Configuration, ContextMap}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade
import org.neo4j.kernel.impl.store.id.RenewableBatchIdSequence
import org.neo4j.kernel.impl.store.record.{PrimitiveRecord, PropertyRecord}
import org.neo4j.kernel.impl.store.{CommonAbstractStore, NeoStores, StandardDynamicRecordAllocator}
import org.neo4j.kernel.impl.transaction.state.RecordAccess

import scala.collection.mutable.{Map => MMap}

/**
  * Created by bluejoe on 2019/4/16.
  */
object InstanceContext {
  @deprecated("warning: no InstanceContext")
  val none: ContextMap = new ContextMap();

  def of(o: AnyRef, path: String): ContextMap = of(o._get(path));

  def of(x: NeoStores): ContextMap = x._get("config").asInstanceOf[Config].getInstanceContext;

  def of(x: Config): ContextMap = x.getInstanceContext;

  def of(x: CommonAbstractStore[_, _]): ContextMap = x._get("configuration").asInstanceOf[Config].getInstanceContext;

  def of(x: RecordAccess[PropertyRecord, PrimitiveRecord]): ContextMap = x._get("loader.val$store.configuration").asInstanceOf[Config].getInstanceContext;

  def of(x: GraphDatabaseFacade): ContextMap = x._get("config").asInstanceOf[Config].getInstanceContext;

  def of(x: QueryState): ContextMap = x.query.transactionalContext._get("inner.tc.kernel.config").asInstanceOf[Config].getInstanceContext;

  def of(o: AnyRef): ContextMap = o match {
    case x: QueryState =>
      of(x);

    case x: StandardDynamicRecordAllocator =>
      of(x._get("idGenerator"));

    case x: NeoStores =>
      of(x);

    case x: Config =>
      of(x);

    case x: CommonAbstractStore[_, _] =>
      of(x);

    case x: RecordAccess[PropertyRecord, PrimitiveRecord] =>
      of(x);

    case x: RenewableBatchIdSequence =>
      of(x._get("source"));

    case x: GraphDatabaseFacade =>
      of(x);

    case _ =>
      throw new FaileToGetInstanceContextException(o);
  }

}

class FaileToGetInstanceContextException(o: AnyRef) extends RuntimeException {

}

object Neo4jConfigUtils {
  implicit def neo4jConfig2Config(neo4jConf: Config) = new Configuration() {
    override def getRaw(name: String): Option[String] = {
      val raw = neo4jConf.getRaw(name);
      if (raw.isPresent) {
        Some(raw.get())
      }
      else {
        None
      }
    }
  }

  implicit def neo4jConfig2Ex(neo4jConf: Config) = ConfigUtils.config2Ex(neo4jConfig2Config(neo4jConf));
}