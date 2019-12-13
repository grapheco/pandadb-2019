package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.interpreted._
import org.neo4j.cypher.internal.v3_5.util.attribution.Id
import org.neo4j.values.virtual.NodeValue

case class AllNodesScanPipe(ident: String)(val id: Id = Id.INVALID_ID) extends PPDPipe {

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val baseContext = state.newExecutionContext(executionContextFactory)
    var nodes: Iterator[NodeValue] = null
    if (nodeStore.isDefined && predicate.isDefined && fatherPipe.isDefined) {
      nodes = fetchNodes(state, baseContext)
    }
    if (nodes==null) {
      nodes = state.query.nodeOps.all
    }
    nodes.map(n => executionContextFactory.copyWith(baseContext, ident, n))
  }
}