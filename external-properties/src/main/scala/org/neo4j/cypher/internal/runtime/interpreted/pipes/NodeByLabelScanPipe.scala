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
import org.neo4j.values.virtual.NodeValue

case class NodeByLabelScanPipe(ident: String, label: LazyLabel)
                              (val id: Id = Id.INVALID_ID) extends PredicatePushDownPipe  {

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {

    label.getOptId(state.query) match {
      case Some(labelId) =>
        val baseContext = state.newExecutionContext(executionContextFactory)
        var nodes: Option[Iterable[NodeValue]] = None
        if (nodeStore.isDefined && fatherPipe != null) {
          nodes = fetchNodes(state, baseContext)
        }
        val nodesIterator: Iterator[NodeValue] = nodes match {
          case Some(x) =>
            x.iterator
          case None =>
            state.query.getNodesByLabel(labelId.id)
        }
        nodesIterator.map(n => executionContextFactory.copyWith(baseContext, ident, n))
      case None =>
        Iterator.empty
    }
  }

}
