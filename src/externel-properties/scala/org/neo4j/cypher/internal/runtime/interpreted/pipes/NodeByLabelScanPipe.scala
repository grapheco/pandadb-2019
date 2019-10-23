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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.v3_5.util.attribution.Id
import org.neo4j.driver.internal.value.NodeValue

import scala.collection.mutable
import org.neo4j.kernel.impl.CustomPropertyNodeStoreHolder
import org.neo4j.values.virtual.{NodeValue => VirtualNodeValue}

case class NodeByLabelScanPipe(ident: String, label: LazyLabel)
                              (val id: Id = Id.INVALID_ID) extends Pipe  {

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {

    /*
    label.getOptId(state.query) match {
      case Some(labelId) =>
        val nodes = state.query.getNodesByLabel(labelId.id)
        nodes.foreach(n=>println(n.id()))
        val baseContext = state.newExecutionContext(executionContextFactory)
        nodes.map(n => executionContextFactory.copyWith(baseContext, ident, n))
      case None =>
        Iterator.empty
    }
    */
    // todo: optimize
    label.getOptId(state.query) match {
      case Some(labelId) =>
        //val nodes = state.query.getNodesByLabel(labelId.id)
        val customPropertyNodes = CustomPropertyNodeStoreHolder.get.getNodesByLabel(label.name)
        val nodesArray = mutable.ArrayBuffer[VirtualNodeValue]()
        customPropertyNodes.foreach(v=>{
          nodesArray.append(v.toNeo4jNodeValue())
        })
        val nodes = nodesArray.toIterator
        val baseContext = state.newExecutionContext(executionContextFactory)
        nodes.map(n => executionContextFactory.copyWith(baseContext, ident, n))
      case None =>
        Iterator.empty
    }
  }

}
