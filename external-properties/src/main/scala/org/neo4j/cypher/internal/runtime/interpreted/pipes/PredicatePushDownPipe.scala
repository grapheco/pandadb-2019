package org.neo4j.cypher.internal.runtime.interpreted.pipes

import cn.pandadb.externalprops.CustomPropertyNodeStore
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{Expression, ParameterExpression, Property, SubstringFunction, ToIntegerFunction}
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates._
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
//      case SubstringFunction(a: Property, b: ParameterExpression) =>
//        NFSubstringFunction(a.propertyKey.name, b.apply(baseContext, state).asInstanceOf[StringValue].stringValue())
//      case ToIntegerFunction(a: Property, b: ParameterExpression) =>
//        NFToIntegerFunction(a.propertyKey.name, b.apply(baseContext, state).asInstanceOf[StringValue].stringValue())
      case x: Ands =>
        convertComboPredicatesLoop(NFAnd, x.predicates, state, baseContext)
      case x: Ors =>
        convertComboPredicatesLoop(NFOr, x.predicates, state, baseContext)
      case Not(p) =>
        NFNot(convertPredicate(p, state, baseContext))
      case _ =>
        null
    }
    expr
  }

  private def convertComboPredicatesLoop(f: (NFPredicate, NFPredicate) => NFPredicate,
                                   expression: NonEmptyList[Predicate],
                                   state: QueryState,
                                   baseContext: ExecutionContext): NFPredicate = {
    val left = convertPredicate(expression.head, state, baseContext)
    val right = if (expression.tailOption.isDefined) convertComboPredicatesLoop(f, expression.tailOption.get, state, baseContext) else null
    if (right == null) {
      left
    }
    else {
      f(left, right)
    }
  }

  def fetchNodes(state: QueryState, baseContext: ExecutionContext): Option[Iterable[NodeValue]] = {
    predicate match {
      case Some(p) =>
        val expr: NFPredicate = convertPredicate(p, state, baseContext)
        if (expr != null) {
          fatherPipe.get.bypass()
          if (labelName != null) {
            Some(nodeStore.get.getNodeBylabelAndFilter(labelName, expr).map(_.toNeo4jNodeValue()))
          }
          else {
            Some(nodeStore.get.filterNodes(expr).map(_.toNeo4jNodeValue()))
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
