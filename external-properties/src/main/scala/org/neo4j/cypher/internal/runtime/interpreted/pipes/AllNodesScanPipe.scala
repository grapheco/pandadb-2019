package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.interpreted._
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{Expression, ParameterExpression, Property}
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates._
import org.neo4j.cypher.internal.v3_5.util.attribution.Id
import org.neo4j.values.virtual.NodeValue
import cn.pandadb.server.GlobalContext
import cn.pandadb.externalprops.CustomPropertyNodeStore

case class AllNodesScanPipe(ident: String)(val id: Id = Id.INVALID_ID) extends Pipe {

  var _optPredicate: Option[Expression] = None;

  def predicatePushDown(predicate: Expression): Unit = {
    _optPredicate = Some(predicate);
  }

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val baseContext = state.newExecutionContext(executionContextFactory)
    val nodeStoreOption: Option[CustomPropertyNodeStore] = GlobalContext.getOption(classOf[CustomPropertyNodeStore].getName)
    var nodes: Iterator[NodeValue] = null
    if (nodeStoreOption.isDefined) {
      val nodeStore: CustomPropertyNodeStore = nodeStoreOption.get
        nodes = _optPredicate match {
        case Some(predicate) =>
          predicate match {
            case GreaterThan(a: Property, b: ParameterExpression) =>
              val value = b.apply(baseContext, state)
              nodeStore.filterNodes(NFGreaterThan(a.propertyKey.name, value)).
                map(_.toNeo4jNodeValue()).iterator

            case GreaterThanOrEqual(a: Property, b: ParameterExpression) =>
              val value = b.apply(baseContext, state)
              nodeStore.filterNodes(NFGreaterThanOrEqual(a.propertyKey.name, value)).
                map(_.toNeo4jNodeValue()).iterator

            case LessThan(a: Property, b: ParameterExpression) =>
              val value = b.apply(baseContext, state)
              nodeStore.filterNodes(NFLessThan(a.propertyKey.name, value)).
                map(_.toNeo4jNodeValue()).iterator

            case LessThanOrEqual(a: Property, b: ParameterExpression) =>
              val value = b.apply(baseContext, state)
              nodeStore.filterNodes(NFLessThanOrEqual(a.propertyKey.name, value)).
                map(_.toNeo4jNodeValue()).iterator

            case Equals(a: Property, b: ParameterExpression) =>
              val value = b.apply(baseContext, state)
              nodeStore.filterNodes(NFEquals(a.propertyKey.name, value)).
                map(_.toNeo4jNodeValue()).iterator
          }
        case _ => state.query.nodeOps.all
      }}
    else {
      nodes = state.query.nodeOps.all
    }

    nodes.map(n => executionContextFactory.copyWith(baseContext, ident, n))
  }
}
