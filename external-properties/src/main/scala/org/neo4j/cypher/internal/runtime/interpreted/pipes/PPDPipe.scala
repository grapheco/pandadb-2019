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

  def fetchNodes(state: QueryState, baseContext: ExecutionContext, labelName: String = null): Iterator[NodeValue] = {
    _optFatherPipe.get.bypass(true)
    if ( _optPredicate.isDefined ) {
      val expr: NFPredicate = _optPredicate.get match {
        case GreaterThan(a: Property, b: ParameterExpression) =>
          NFGreaterThan(a.propertyKey.name, b.apply(baseContext, state))
        case GreaterThanOrEqual(a: Property, b: ParameterExpression) =>
          NFGreaterThanOrEqual(a.propertyKey.name, b.apply(baseContext, state))
        case LessThan(a: Property, b: ParameterExpression) =>
          NFLessThan(a.propertyKey.name, b.apply(baseContext, state))
        case LessThanOrEqual(a: Property, b: ParameterExpression) =>
          NFLessThanOrEqual(a.propertyKey.name, b.apply(baseContext, state))
        case Equals(a: Property, b: ParameterExpression) =>
          NFEquals(a.propertyKey.name, b.apply(baseContext, state))
        case Contains(a: Property, b: ParameterExpression) =>
          NFContainsWith(a.propertyKey.name, b.apply(baseContext, state).asInstanceOf[StringValue].stringValue())
        case StartsWith(a: Property, b: ParameterExpression) =>
          NFStartsWith(a.propertyKey.name, b.apply(baseContext, state).asInstanceOf[StringValue].stringValue())
        case EndsWith(a: Property, b: ParameterExpression) =>
          NFEndsWith(a.propertyKey.name, b.apply(baseContext, state).asInstanceOf[StringValue].stringValue())
        case RegularExpression(a: Property, b: ParameterExpression) =>
          NFRegexp(a.propertyKey.name, b.apply(baseContext, state).asInstanceOf[StringValue].stringValue())
        case _ =>
          _optFatherPipe.get.bypass(false)
          null
      }

      if (expr != null) {
        if (labelName != null) {
          _optNodeStore.get.getNodeBylabelAndfilter(labelName, expr).map(_.toNeo4jNodeValue()).iterator
        }
        else {
          _optNodeStore.get.filterNodes(expr).map(_.toNeo4jNodeValue()).iterator
        }
      }
      else {
        if (labelName != null) {
          _optNodeStore.get.getNodesByLabel(labelName).map(_.toNeo4jNodeValue()).iterator
        }
        else {
          null
        }
      }
    }
    else {
      null
    }
  }
}
