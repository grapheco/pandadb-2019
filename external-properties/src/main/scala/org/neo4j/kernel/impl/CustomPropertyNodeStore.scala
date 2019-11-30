package org.neo4j.kernel.impl

import cn.pandadb.context.{InstanceBoundService, InstanceBoundServiceContext}
import cn.pandadb.util.PandaException
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
  /*
  def deleteNodes(docsToBeDeleted: Iterable[Long]);

  def addNodes(docsToAdded: Iterable[NodeWithProperties]);

  def updateNodes(docsToUpdated: Iterable[CustomPropertyNodeModification]);
  */
  def beginWriteTransaction(): ExternalPropertyWriteTransaction;

  def filterNodes(expr: NFPredicate): Iterable[NodeWithProperties];

  def getNodesByLabel(label: String): Iterable[NodeWithProperties];

  def getNodeById(id: Long): Option[NodeWithProperties];
}

trait ExternalPropertyWriteTransaction {
  def deleteNodes(docsToBeDeleted: Iterable[Long]);

  def addNodes(docsToAdded: Iterable[NodeWithProperties]);

  def addProperty(nodeId: Long, properties: (String, Value)*);

  def removeProperty(nodeId: Long, propertyNames: String*);

  def updateProperty(nodeId: Long, properties: (String, Value)*);

  @throws[FailedToPrepareTransaction]
  def prepare(): PreparedExternalPropertyWriteTransaction;
}

trait PreparedExternalPropertyWriteTransaction {
  @throws[FailedToCommitTransaction]
  def commit(): Unit;

  def rollback(): Unit;
}

class FailedToPrepareTransaction(tx: ExternalPropertyWriteTransaction) extends PandaException("failed to prepare transaction: $tx") {

}

class FailedToCommitTransaction(tx: PreparedExternalPropertyWriteTransaction) extends PandaException("failed to commit transaction: $tx") {

}

/*
case class CustomPropertyNodeModification(
                                           id: Long,
                                           fieldsAdded: Map[String, Value],
                                           fieldsRemoved: Iterable[String],
                                           fieldsUpdated: Map[String, Value],
                                           labelsAdded: Iterable[String],
                                           labelsRemoved: Iterable[String]) {

}
*/

case class NodeWithProperties(id: Long, var fields: Map[String, Value], var labels: Iterable[String]) {
  def field(name: String): Option[Value] = fields.get(name)

  def toNeo4jNodeValue(): NodeValue = {
    VirtualValues.nodeValue(id,
      Values.stringArray(labels.toArray: _*),
      VirtualValues.map(fields.keys.toArray, fields.values.toArray))
  }
}
