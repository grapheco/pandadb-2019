package cn.pandadb.externalprops

import java.util.{Map => JMap}

import cn.pandadb.context.{InstanceBoundService, InstanceBoundServiceContext}
import cn.pandadb.util.PandaException
import org.neo4j.cypher.internal.runtime.interpreted.NFPredicate
import org.neo4j.values.storable.{Value, Values}
import org.neo4j.values.virtual.{NodeValue, VirtualValues}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bluejoe on 2019/10/7.
  */
trait ExternalPropertyStoreFactory {
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
  def deleteNode(nodeId: Long);

  def addNode(nodeId: Long);

  def addProperty(nodeId: Long, key: String, value: Value): Unit;

  def removeProperty(nodeId: Long, key: String);

  def updateProperty(nodeId: Long, key: String, value: Value): Unit;

  def addLabel(nodeId: Long, label: String): Unit;

  def removeLabel(nodeId: Long, label: String): Unit;

  @throws[FailedToPrepareTransaction]
  def prepare(): PreparedExternalPropertyWriteTransaction;
}

trait PreparedExternalPropertyWriteTransaction {
  @throws[FailedToCommitTransaction]
  def commit(): Unit;

  def rollback(): Unit;
}

class FailedToPrepareTransaction(tx: ExternalPropertyWriteTransaction, cause: Throwable)
  extends PandaException("failed to prepare transaction: $tx") {

}

class FailedToCommitTransaction(tx: PreparedExternalPropertyWriteTransaction, cause: Throwable)
  extends PandaException("failed to commit transaction: $tx") {

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

/**
  * buffer based implementation of ExternalPropertyWriteTransaction
  * this is a template class which should be derived
  */
abstract class BufferedExternalPropertyWriteTransaction() extends ExternalPropertyWriteTransaction {
  val buffer = ArrayBuffer[PropertyModificationCommand]();

  override def deleteNode(nodeId: Long): Unit = buffer += DeleteNodeCommand(nodeId)

  override def addNode(nodeId: Long): Unit = buffer += AddNodeCommand(nodeId)

  override def addProperty(nodeId: Long, key: String, value: Value): Unit = buffer += AddPropertyCommand(nodeId, key, value)

  override def removeProperty(nodeId: Long, key: String): Unit = buffer += RemovePropertyCommand(nodeId, key)

  override def updateProperty(nodeId: Long, key: String, value: Value): Unit = buffer += UpdatePropertyCommand(nodeId, key, value)

  override def addLabel(nodeId: Long, label: String): Unit = buffer += AddLabelCommand(label)

  override def removeLabel(nodeId: Long, label: String): Unit = buffer += RemoveLabelCommand(label)

  @throws[FailedToPrepareTransaction]
  override def prepare(): PreparedExternalPropertyWriteTransaction = {
    val combinedCommands = buffer.toArray;
    internalCheck(combinedCommands).map { e =>
      throw new FailedToPrepareTransaction(this, e);
    }.getOrElse {
      new PreparedExternalPropertyWriteTransaction() {
        @throws[FailedToCommitTransaction]
        override def commit(): Unit = internalCommit(combinedCommands)

        override def rollback(): Unit = internalRollback(combinedCommands)
      }
    }
  }

  def internalCommit(commands: Array[PropertyModificationCommand]);

  def internalRollback(commands: Array[PropertyModificationCommand]);

  def internalCheck(commands: Array[PropertyModificationCommand]): Option[Throwable];
}

trait PropertyModificationCommand {

}

case class DeleteNodeCommand(nodeId: Long) extends PropertyModificationCommand {

}

case class AddNodeCommand(nodeId: Long) extends PropertyModificationCommand {

}

case class UpdatePropertyCommand(nodeId: Long, key: String, value: Value) extends PropertyModificationCommand {

}

case class RemovePropertyCommand(nodeId: Long, key: String) extends PropertyModificationCommand {

}

case class AddPropertyCommand(nodeId: Long, key: String, value: Value) extends PropertyModificationCommand {

}

case class AddLabelCommand(nodeId: Long, key: String) extends PropertyModificationCommand {

}

case class RemoveLabelCommand(nodeId: Long, key: String) extends PropertyModificationCommand {

}