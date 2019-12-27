package org.neo4j.cypher.internal.runtime.interpreted.pipes

import cn.pandadb.externalprops.CustomPropertyNodeStore
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{Expression, ParameterExpression, Property}
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates._
import org.neo4j.cypher.internal.runtime.interpreted._
import org.neo4j.cypher.internal.v3_5.util.{Fby, Last, NonEmptyList}
import org.neo4j.values.storable.StringValue
import org.neo4j.values.virtual.NodeValue

trait PredicatePushDownPipe extends Pipe{

  var nodeStore: Option[CustomPropertyNodeStore] = None

  var fatherPipe: Option[FilterPipe] = None

  var predicate: Option[Expression] = None

  var labelName: String = null

  def pushDownPredicate(nodeStore: CustomPropertyNodeStore, fatherPipe: FilterPipe, predicate: Expression, label: String = null): Unit = {
    this.nodeStore = Some(nodeStore)
    this.fatherPipe = Some(fatherPipe)
    this.predicate = Some(predicate)
    this.labelName = label
  }

  private def convertNaivePredicate(expression: Expression, state: QueryState, baseContext: ExecutionContext): NFPredicate = {
    val expr: NFPredicate = expression match {
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
        null
    }
    expr
  }

  private def convertAndsPredicate(expression: NonEmptyList[Predicate], state: QueryState, baseContext: ExecutionContext): NFPredicate = {
    val left = convertNaivePredicate(expression.head, state, baseContext)
    val right = if (expression.tailOption.isDefined) convertAndsPredicate(expression.tailOption.get, state, baseContext) else null
    if (right == null) {
      left
    }
    else {
      NFAnd(left, right)
    }
  }

  def fetchNodes(state: QueryState, baseContext: ExecutionContext): Iterator[NodeValue] = {
    if (predicate.isDefined) {
      val expr: NFPredicate = predicate.get match {
        case x: Ands =>
          convertAndsPredicate(x.predicates, state, baseContext)
        case _ =>
          convertNaivePredicate(predicate.get, state, baseContext)
      }
      println(expr)
      if (expr != null) {
        fatherPipe.get.bypass()
        if (labelName != null) {
          nodeStore.get.getNodeBylabelAndfilter(labelName, expr).map(_.toNeo4jNodeValue()).iterator
        }
        else {
          nodeStore.get.filterNodes(expr).map(_.toNeo4jNodeValue()).iterator
        }
      }
      else {
        if (labelName != null) {
          nodeStore.get.getNodesByLabel(labelName).map(_.toNeo4jNodeValue()).iterator
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
