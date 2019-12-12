package org.neo4j.cypher.internal.runtime.interpreted.pipes

import cn.pandadb.externalprops.CustomPropertyNodeStore
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{Expression, ParameterExpression, Property}
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates._
import org.neo4j.cypher.internal.runtime.interpreted._
import org.neo4j.values.storable.{StringValue}
import org.neo4j.values.virtual.NodeValue

trait PPDPipe extends Pipe{

  var _optNodeStore: Option[CustomPropertyNodeStore] = None

  var _optPredicate: Option[Expression] = None

  var _optFatherPipe: Option[FilterPipe] = None

  def predicatePushDown(nodeStore: CustomPropertyNodeStore, predicate: Expression, fatherPipe: FilterPipe): Unit = {
    _optPredicate = Some(predicate)
    _optNodeStore = Some(nodeStore)
    _optFatherPipe = Some(fatherPipe)

  }

  def fetchNodes(state: QueryState, baseContext: ExecutionContext): Iterator[NodeValue] = {
    _optFatherPipe.get.bypass(true)
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

        case Contains(a: Property, b: ParameterExpression) =>
          val value = b.apply(baseContext, state)
          _optNodeStore.get.filterNodes(NFContainsWith(a.propertyKey.name, value.asInstanceOf[String])).
            map(_.toNeo4jNodeValue()).iterator

        case StartsWith(a: Property, b: ParameterExpression) =>
          val value = b.apply(baseContext, state)
          _optNodeStore.get.filterNodes(NFStartsWith(a.propertyKey.name, value.asInstanceOf[StringValue].stringValue())).
            map(_.toNeo4jNodeValue()).iterator

        case EndsWith(a: Property, b: ParameterExpression) =>
          val value = b.apply(baseContext, state)
          _optNodeStore.get.filterNodes(NFEndsWith(a.propertyKey.name, value.asInstanceOf[StringValue].stringValue())).
            map(_.toNeo4jNodeValue()).iterator

        case RegularExpression(a: Property, b: ParameterExpression) =>
          val value = b.apply(baseContext, state)
          _optNodeStore.get.filterNodes(NFRegexp(a.propertyKey.name, value.asInstanceOf[StringValue].stringValue())).
            map(_.toNeo4jNodeValue()).iterator

        case _ =>
          _optFatherPipe.get.bypass(false)
          null
      }
  }
}
