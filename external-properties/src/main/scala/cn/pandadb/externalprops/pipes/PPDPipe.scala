/*
 * Copyright (c) 2002-2019 "PandaDB"
 */
package cn.pandadb.externalprops.pipes

import cn.pandadb.externalprops.CustomPropertyNodeStore
import cn.pandadb.server.GlobalContext
import org.neo4j.cypher.internal.runtime.interpreted.{ExecutionContext, NFEquals, NFGreaterThan, NFGreaterThanOrEqual, NFLessThan, NFLessThanOrEqual}
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{Expression, ParameterExpression, Property}
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.{Equals, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, QueryState}
import org.neo4j.cypher.internal.v3_5.util.InternalException
import org.neo4j.values.virtual.NodeValue

abstract class PPDPipe(predicate: Expression) extends Pipe{

  def fetchNodes(state: QueryState): Iterator[NodeValue] = {
    val baseContext = state.newExecutionContext(executionContextFactory)
    val nodeStoreOption: Option[CustomPropertyNodeStore] = GlobalContext.getOption(classOf[CustomPropertyNodeStore].getName)
    if (nodeStoreOption.isDefined) {
      val nodeStore: CustomPropertyNodeStore = nodeStoreOption.get
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
        case _ => throw new InternalException(s"undefined predicate ${predicate}")
      }
    }
    else {
      throw new InternalException("undefined CustomPropertyNodeStore")
    }
  }
}