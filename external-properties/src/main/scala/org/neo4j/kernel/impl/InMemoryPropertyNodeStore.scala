package org.neo4j.kernel.impl

import cn.pandadb.context.InstanceBoundServiceContext
import org.neo4j.cypher.internal.runtime.interpreted._
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.NumberValue

import scala.collection.mutable

/**
  * Created by bluejoe on 2019/10/7.
  */
class InMemoryPropertyNodeStoreFactory extends PropertyStoreFactory {
  override def create(ctx: InstanceBoundServiceContext): CustomPropertyNodeStore = InMemoryPropertyNodeStore;
}

/**
  * used for unit test
  */
object InMemoryPropertyNodeStore extends CustomPropertyNodeStore {
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

  override def updateNodes(docsToUpdated: Iterable[CustomPropertyNodeModification]): Unit = {
    docsToUpdated.foreach(d => {
      val n: CustomPropertyNode = nodes(d.id)
      if (d.fieldsAdded != null && d.fieldsAdded.size>0) {
        nodes(d.id).fields ++= d.fieldsAdded
      }
      if (d.fieldsUpdated != null && d.fieldsUpdated.size>0) {
        nodes(d.id).fields ++= d.fieldsUpdated
      }
      if (d.fieldsRemoved != null && d.fieldsRemoved.size>0) {
        d.fieldsRemoved.foreach(f => nodes(d.id).fields -= f)
      }
      if (d.labelsAdded != null && d.labelsAdded.size>0) {
        nodes(d.id).labels ++= d.labelsAdded
        nodes(d.id).labels = nodes(d.id).labels.toSet
      }
      if (d.labelsRemoved != null && d.labelsRemoved.size>0) {
        val tmpLabels = nodes(d.id).labels.toSet
        nodes(d.id).labels = tmpLabels -- d.labelsRemoved.toSet
      }

    })
  }

  override def getNodesByLabel(label: String): Iterable[CustomPropertyNode] = {
    val res = mutable.ArrayBuffer[CustomPropertyNode]()
    nodes.map(n => {
      if (n._2.labels.toArray.contains(label)) {
        res.append(n._2)
      }

    })
    res
  }

  override def getNodeById(id: Long): Option[CustomPropertyNode] = {
    nodes.get(id)
  }

  override def start(ctx: InstanceBoundServiceContext): Unit = {
    nodes.clear()
  }

  override def stop(ctx: InstanceBoundServiceContext): Unit = {
    nodes.clear()
  }
}
