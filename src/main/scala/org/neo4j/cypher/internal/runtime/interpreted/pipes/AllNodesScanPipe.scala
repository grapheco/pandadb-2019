package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{Expression, ParameterExpression, Property}
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.{GreaterThan, LessThan}
import org.neo4j.cypher.internal.runtime.interpreted.{ExecutionContext, NodeFieldGreaterThan, NodeFieldLessThan}
import org.neo4j.cypher.internal.v3_5.util.attribution.Id
import org.neo4j.kernel.impl.CustomPropertyNodeStoreHolder
import org.neo4j.values.virtual.NodeValue

case class AllNodesScanPipe(ident: String)(val id: Id = Id.INVALID_ID) extends Pipe {

  var _optPredicate: Option[Expression] = None;

  def predicatePushDown(predicate: Expression): Unit = {
    _optPredicate = Some(predicate);
  }

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val baseContext = state.newExecutionContext(executionContextFactory)
    val nodes: Iterator[NodeValue] = _optPredicate match {
      case Some(predicate) =>
        predicate match {
          case GreaterThan(a: Property, b: ParameterExpression) => {
            val value = b.apply(baseContext, state)
            CustomPropertyNodeStoreHolder.get.filterNodes(NodeFieldGreaterThan(a.propertyKey.name, value)).
              map(_.toNeo4jNodeValue()).iterator
          }
          case LessThan(a: Property, b: ParameterExpression) => {
            val value = b.apply(baseContext, state)
            CustomPropertyNodeStoreHolder.get.filterNodes(NodeFieldLessThan(a.propertyKey.name, value)).
              map(_.toNeo4jNodeValue()).iterator
          }
        }

      case _ => state.query.nodeOps.all
    }

    nodes.map(n => executionContextFactory.copyWith(baseContext, ident, n))
  }
}
