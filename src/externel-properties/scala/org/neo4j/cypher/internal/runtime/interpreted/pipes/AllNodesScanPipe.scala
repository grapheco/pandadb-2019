package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{Expression, ParameterExpression, Property}
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.{GreaterThan, GreaterThanOrEqual}
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.{LessThan, LessThanOrEqual, Equals}
import org.neo4j.cypher.internal.runtime.interpreted._
import org.neo4j.cypher.internal.v3_5.util.attribution.Id
import org.neo4j.kernel.impl.CustomPropertyNodeStoreHolder
import org.neo4j.values.virtual.NodeValue

case class AllNodesScanPipe(ident: String)(val id: Id = Id.INVALID_ID) extends Pipe {

  // NOTE: graiph
  var _optPredicate: Option[Expression] = None;

  def predicatePushDown(predicate: Expression): Unit = {
    _optPredicate = Some(predicate);
  }
  // END-NOTE


  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val baseContext = state.newExecutionContext(executionContextFactory)
    val nodes: Iterator[NodeValue] = _optPredicate match {
      case Some(predicate) =>
        predicate match {
          case GreaterThan(a: Property, b: ParameterExpression) => {
            val value = b.apply(baseContext, state)
            CustomPropertyNodeStoreHolder.get.filterNodes(NFGreaterThan(a.propertyKey.name, value)).
              map(_.toNeo4jNodeValue()).iterator
          }

          case GreaterThanOrEqual(a: Property, b: ParameterExpression) => {
            val value = b.apply(baseContext, state)
            CustomPropertyNodeStoreHolder.get.filterNodes(NFGreaterThanOrEqual(a.propertyKey.name, value)).
              map(_.toNeo4jNodeValue()).iterator
          }

          case LessThan(a: Property, b: ParameterExpression) => {
            val value = b.apply(baseContext, state)
            CustomPropertyNodeStoreHolder.get.filterNodes(NFLessThan(a.propertyKey.name, value)).
              map(_.toNeo4jNodeValue()).iterator
          }

          case LessThanOrEqual(a: Property, b: ParameterExpression) => {
            val value = b.apply(baseContext, state)
            CustomPropertyNodeStoreHolder.get.filterNodes(NFLessThanOrEqual(a.propertyKey.name, value)).
              map(_.toNeo4jNodeValue()).iterator
          }

          case Equals(a: Property, b: ParameterExpression) => {
            val value = b.apply(baseContext, state)
            CustomPropertyNodeStoreHolder.get.filterNodes(NFEquals(a.propertyKey.name, value)).
              map(_.toNeo4jNodeValue()).iterator
          }

          case _ => state.query.nodeOps.all

        }

      case _ => state.query.nodeOps.all
    }

    nodes.map(n => executionContextFactory.copyWith(baseContext, ident, n))
  }
}
