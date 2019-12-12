package org.neo4j.cypher.internal.runtime.interpreted.pipes

import cn.pandadb.externalprops.CustomPropertyNodeStore
import cn.pandadb.server.GlobalContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{Expression, ParameterExpression, Property}
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates._
import org.neo4j.cypher.internal.runtime.interpreted._
import org.neo4j.cypher.internal.v3_5.util.InternalException
import org.neo4j.values.virtual.NodeValue

trait PPDPipe extends Pipe{


  var _optNodeStore: Option[CustomPropertyNodeStore] = None

  var _optPredicate: Option[Expression] = None;

  def predicatePushDown(nodeStore: CustomPropertyNodeStore, predicate: Expression): Unit = {
    _optPredicate = Some(predicate);
    _optNodeStore = Some(nodeStore)
  }

  def fetchNodes(state: QueryState, baseContext: ExecutionContext): Iterator[NodeValue] = {
    _optPredicate.get match {
        case GreaterThan(a: Property, b: ParameterExpression) =>
          val value = b.apply(baseContext, state)
          _optNodeStore.get.filterNodes(NFGreaterThan(a.propertyKey.name, value)).
            map(_.toNeo4jNodeValue()).iterator

        case GreaterThanOrEqual(a: Property, b: ParameterExpression) =>
          val value = b.apply(baseContext, state)
          _optNodeStore.get.filterNodes(NFGreaterThanOrEqual(a.propertyKey.name, value)).
            map(_.toNeo4jNodeValue()).iterator

        case LessThan(a: Property, b: ParameterExpression) =>
          val value = b.apply(baseContext, state)
          _optNodeStore.get.filterNodes(NFLessThan(a.propertyKey.name, value)).
            map(_.toNeo4jNodeValue()).iterator

        case LessThanOrEqual(a: Property, b: ParameterExpression) =>
          val value = b.apply(baseContext, state)
          _optNodeStore.get.filterNodes(NFLessThanOrEqual(a.propertyKey.name, value)).
            map(_.toNeo4jNodeValue()).iterator

        case Equals(a: Property, b: ParameterExpression) =>
          val value = b.apply(baseContext, state)
          _optNodeStore.get.filterNodes(NFEquals(a.propertyKey.name, value)).
            map(_.toNeo4jNodeValue()).iterator
        case _ => throw new InternalException(s"undefined predicate")
      }
  }
}
