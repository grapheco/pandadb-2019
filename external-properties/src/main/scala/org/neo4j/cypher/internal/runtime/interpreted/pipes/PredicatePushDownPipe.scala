package org.neo4j.cypher.internal.runtime.interpreted.pipes

import cn.pandadb.externalprops.CustomPropertyNodeStore
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{Expression, ParameterExpression, Property, SubstringFunction, ToIntegerFunction}
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates._
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.KeyToken
import org.neo4j.cypher.internal.runtime.interpreted.{NFPredicate, _}
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

  private def convertPredicate(expression: Expression, state: QueryState, baseContext: ExecutionContext): NFPredicate = {
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
      case PropertyExists(variable: Expression, propertyKey: KeyToken) =>
        NFHasProperty(propertyKey.name)
      case x: Ands =>
        convertComboPredicatesLoop(NFAnd, x.predicates, state, baseContext)
      case x: Ors =>
        convertComboPredicatesLoop(NFOr, x.predicates, state, baseContext)
      case Not(p) =>
        val innerP: NFPredicate = convertPredicate(p, state, baseContext)
        if (innerP == null) null else NFNot(innerP)
      case _ =>
        null
    }
    expr
  }

  private def convertComboPredicatesLoop(f: (NFPredicate, NFPredicate) => NFPredicate,
                                   expression: NonEmptyList[Predicate],
                                   state: QueryState,
                                   baseContext: ExecutionContext): NFPredicate = {
    val lhs = convertPredicate(expression.head, state, baseContext)
    val rhs = if (expression.tailOption.isDefined) convertComboPredicatesLoop(f, expression.tailOption.get, state, baseContext) else null
    if (rhs == null) {
      lhs
    }
    else {
      f(lhs, rhs)
    }
  }

  def fetchNodes(state: QueryState, baseContext: ExecutionContext): Option[Iterable[NodeValue]] = {
    predicate match {
      case Some(p) =>
        val expr: NFPredicate = convertPredicate(p, state, baseContext)
        if (expr != null && (expr.isInstanceOf[NFAnd] || expr.isInstanceOf[NFOr] || expr.isInstanceOf[NFContainsWith])) {// only enable ppd when NFAnd, NFor
          fatherPipe.get.bypass()
          if (labelName != null) {
            Some(nodeStore.get.getNodeWithPropertiesBylabelAndFilter(labelName, expr).map(_.toNeo4jNodeValue()))
          }
          else {
            Some(nodeStore.get.filterNodesWithProperties(expr).map(_.toNeo4jNodeValue()))
          }
        }
        else {
          if (labelName != null) {
            Some(nodeStore.get.getNodesByLabel(labelName).map(_.toNeo4jNodeValue()))
          }
          else {
            None
          }
        }
      case None =>
        None
    }
  }
}
