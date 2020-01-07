package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.interpreted._
import org.neo4j.cypher.internal.v3_5.util.attribution.Id
import org.neo4j.values.virtual.NodeValue

case class AllNodesScanPipe(ident: String)(val id: Id = Id.INVALID_ID) extends PredicatePushDownPipe {

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val baseContext = state.newExecutionContext(executionContextFactory)
    var nodesOption: Option[Iterator[NodeValue]] = None
    if (nodeStore.isDefined && predicate.isDefined && fatherPipe != null) {
      nodesOption = fetchNodes(state, baseContext)
    }
    val nodes: Iterator[NodeValue] = nodesOption match {
      case Some(x) =>
        x
      case None =>
        state.query.nodeOps.all
    }
    nodes.map(n => executionContextFactory.copyWith(baseContext, ident, n))
  }
}