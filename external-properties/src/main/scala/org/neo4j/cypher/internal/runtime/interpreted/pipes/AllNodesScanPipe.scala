package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.interpreted._
import org.neo4j.cypher.internal.v3_5.util.attribution.Id
import org.neo4j.values.virtual.{NodeValue, VirtualNodeValue}

case class AllNodesScanPipe(ident: String)(val id: Id = Id.INVALID_ID) extends PredicatePushDownPipe {

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val baseContext = state.newExecutionContext(executionContextFactory)
    var nodes: Option[Iterable[VirtualNodeValue]] = None
    if (nodeStore.isDefined && predicate.isDefined && fatherPipe != null) {
      nodes = fetchNodes(state, baseContext)
    }
    val nodesIterator = nodes match {
      case Some(x) =>
        x.iterator
      case None =>
        state.query.nodeOps.all
    }
    nodesIterator.map(n => executionContextFactory.copyWith(baseContext, ident, n))
  }
}