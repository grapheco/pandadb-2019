package org.neo4j.kernel.impl

import org.neo4j.cypher.internal.runtime.interpreted._
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.NumberValue

import scala.collection.mutable

/**
  * Created by bluejoe on 2019/10/7.
  */
class InMemoryPropertyNodeStore extends CustomPropertyNodeStore {
  val nodes = mutable.Map[Long, CustomPropertyNode]();

  def filterNodes(expr: NFPredicate): Iterable[CustomPropertyNode] = {
    expr match {
      case NFGreaterThan(fieldName: String, value: AnyValue) => {
        nodes.values.filter(x => x.field(fieldName).map(_.asInstanceOf[NumberValue].doubleValue() >
          value.asInstanceOf[NumberValue].doubleValue()).getOrElse(false))
      }

      case NFLessThan(fieldName: String, value: AnyValue) => {
        nodes.values.filter(x => x.field(fieldName).map(_.asInstanceOf[NumberValue].doubleValue() <
          value.asInstanceOf[NumberValue].doubleValue()).getOrElse(false))
      }

      case NFLessThanOrEqual(fieldName: String, value: AnyValue) => {
        nodes.values.filter(x => x.field(fieldName).map(_.asInstanceOf[NumberValue].doubleValue() <=
          value.asInstanceOf[NumberValue].doubleValue()).getOrElse(false))
      }

      case NFGreaterThanOrEqual(fieldName: String, value: AnyValue) => {
        nodes.values.filter(x => x.field(fieldName).map(_.asInstanceOf[NumberValue].doubleValue() >=
          value.asInstanceOf[NumberValue].doubleValue()).getOrElse(false))
      }

      case NFEquals(fieldName: String, value: AnyValue) => {
        nodes.values.filter(x => x.field(fieldName).map(_.asInstanceOf[NumberValue].doubleValue() ==
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

  override def getNodesByLabel(label: String): Iterable[CustomPropertyNode] = {
    val res = mutable.ArrayBuffer[CustomPropertyNode]()
    nodes.map(n=>{
      if(n._2.labels.toArray.contains(label) )
        res.append(n._2)
    })
    res
  }

}
