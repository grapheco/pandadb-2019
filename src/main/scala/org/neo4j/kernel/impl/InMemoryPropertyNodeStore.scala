package org.neo4j.kernel.impl

import org.neo4j.cypher.internal.runtime.interpreted.{NodeFieldGreaterThan, NodeFieldLessThan, NodeFieldPredicate}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.NumberValue

import scala.collection.mutable

/**
  * Created by bluejoe on 2019/10/7.
  */
class InMemoryPropertyNodeStore extends CustomPropertyNodeStore {
  val nodes = mutable.Map[Long, CustomPropertyNode]();

  def filterNodes(expr: NodeFieldPredicate): Iterable[CustomPropertyNode] = {
    expr match {
      case NodeFieldGreaterThan(fieldName: String, value: AnyValue) => {
        nodes.values.filter(x => x.field(fieldName).map(_.asInstanceOf[NumberValue].doubleValue() >
          value.asInstanceOf[NumberValue].doubleValue()).getOrElse(false))
      }

      case NodeFieldLessThan(fieldName: String, value: AnyValue) => {
        nodes.values.filter(x => x.field(fieldName).map(_.asInstanceOf[NumberValue].doubleValue() <
          value.asInstanceOf[NumberValue].doubleValue()).getOrElse(false))
      }
    }
  }


  override def deleteNodes(docsToBeDeleted: Iterable[Long]): Unit = {
    nodes --= docsToBeDeleted
  }

  override def addNodes(docsToAdded: Iterable[CustomPropertyNode]): Unit = {
    nodes ++= docsToAdded.map(x => x.id -> x)
  }

  override def init(): Unit = {
  }

  override def updateNodes(docsToUpdated: Iterable[CustomPropertyNodeModification]): Unit = {

  }
}
