package org.neo4j.kernel.impl

import cn.graiph.context.{InstanceBoundService, InstanceBoundServiceContext}
import org.neo4j.cypher.internal.runtime.interpreted.NFPredicate
import org.neo4j.values.storable.{Value, Values}
import org.neo4j.values.virtual.{NodeValue, VirtualValues}

/**
  * Created by bluejoe on 2019/10/7.
  */
trait PropertyStoreFactory {
  def create(ctx: InstanceBoundServiceContext): CustomPropertyNodeStore;
}

trait CustomPropertyNodeStore extends InstanceBoundService {
  def deleteNodes(docsToBeDeleted: Iterable[Long]);

  def addNodes(docsToAdded: Iterable[CustomPropertyNode]);

  def updateNodes(docsToUpdated: Iterable[CustomPropertyNodeModification]);

  def filterNodes(expr: NFPredicate): Iterable[CustomPropertyNode];

  def getNodesByLabel(label: String): Iterable[CustomPropertyNode];
}

case class CustomPropertyNodeModification(
                                           id: Long,
                                           fieldsAdded: Map[String, Value],
                                           fieldsRemoved: Iterable[String],
                                           fieldsUpdated: Map[String, Value],
                                           labelsAdded: Iterable[String],
                                           labelsRemoved: Iterable[String]) {

}

case class CustomPropertyNode(id: Long, fields: Map[String, Value], labels: Iterable[String]) {
  def field(name: String): Option[Value] = fields.get(name)

  def toNeo4jNodeValue(): NodeValue = {
    VirtualValues.nodeValue(id,
      Values.stringArray(labels.toArray: _*),
      VirtualValues.map(fields.keys.toArray, fields.values.toArray))
  }
}
