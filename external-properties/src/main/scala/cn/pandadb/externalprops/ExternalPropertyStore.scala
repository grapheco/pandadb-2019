package cn.pandadb.externalprops

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
  def prepareWriteTransaction(): PreparedPropertyWriteTransaction;

  def filterNodes(expr: NFPredicate): Iterable[NodeWithProperties];

  def getNodesByLabel(label: String): Iterable[NodeWithProperties];

  def getNodeById(id: Long): Option[NodeWithProperties];
}

trait PreparedPropertyWriteTransaction {
  def deleteNode(nodeId: Long);

  def addNode(nodeId: Long);

  def addProperty(nodeId: Long, key: String, value: Value): Unit;

  def removeProperty(nodeId: Long, key: String);

  def updateProperty(nodeId: Long, key: String, value: Value): Unit;

  def addLabel(nodeId: Long, label: String): Unit;

  def removeLabel(nodeId: Long, label: String): Unit;

  def startWriteTransaction(): PropertyWriteTransaction;
}

trait PropertyWriteTransaction {
  @throws[FailedToCommitTransaction]
  def commit(): Unit;

  @throws[FailedToRollbackTransaction]
  def rollback(): Unit;
}

class FailedToCommitTransaction(tx: PreparedPropertyWriteTransaction, cause: Throwable)
  extends PandaException("failed to commit transaction: $tx") {

}

class FailedToRollbackTransaction(tx: PreparedPropertyWriteTransaction, cause: Throwable)
  extends PandaException("failed to roll back transaction: $tx") {

}

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
abstract class BufferedExternalPropertyWriteTransaction() extends PreparedPropertyWriteTransaction {
  val buffer = ArrayBuffer[PropertyModificationCommand]();

  override def deleteNode(nodeId: Long): Unit = buffer += DeleteNodeCommand(nodeId)

  override def addNode(nodeId: Long): Unit = buffer += AddNodeCommand(nodeId)

  override def addProperty(nodeId: Long, key: String, value: Value): Unit = buffer += AddPropertyCommand(nodeId, key, value)

  override def removeProperty(nodeId: Long, key: String): Unit = buffer += RemovePropertyCommand(nodeId, key)

  override def updateProperty(nodeId: Long, key: String, value: Value): Unit = buffer += UpdatePropertyCommand(nodeId, key, value)

  override def addLabel(nodeId: Long, label: String): Unit = buffer += AddLabelCommand(nodeId, label)

  override def removeLabel(nodeId: Long, label: String): Unit = buffer += RemoveLabelCommand(nodeId, label)

  override def startWriteTransaction(): PropertyWriteTransaction =
    new VisitorPreparedTransaction(combinedCommands(), commitPerformer(), rollbackPerformer())

  def combinedCommands(): CombinedTransactionCommands = {
    null
  }

  def commitPerformer(): CombinedTransactionCommandVisitor;

  def rollbackPerformer(): CombinedTransactionCommandVisitor;
}

class VisitorPreparedTransaction(
                                  combinedCommands: CombinedTransactionCommands,
                                  commitPerformer: CombinedTransactionCommandVisitor,
                                  rollbackPerformer: CombinedTransactionCommandVisitor)
  extends PropertyWriteTransaction {

  @throws[FailedToCommitTransaction]
  override def commit(): Unit = {
    commitPerformer.start(combinedCommands)
    combinedCommands.accepts(commitPerformer)
    commitPerformer.end(combinedCommands)
  }

  @throws[FailedToRollbackTransaction]
  override def rollback(): Unit = {
    rollbackPerformer.start(combinedCommands)
    combinedCommands.accepts(rollbackPerformer)
    rollbackPerformer.end(combinedCommands)
  }

  def combineCommands(commands: Array[PropertyModificationCommand]): CombinedTransactionCommands = {
    CombinedTransactionCommands(commands)
  }
}

case class CombinedTransactionCommands(commands: Array[PropertyModificationCommand]) {
  //commands-->combined
  def accepts(visitor: CombinedTransactionCommandVisitor): Unit = {

  }
}

trait CombinedTransactionCommandVisitor {
  def start(commands: CombinedTransactionCommands);

  def end(commands: CombinedTransactionCommands);

  def visitAddNode(nodeId: Long, props: Map[String, Value], labels: Array[String]);

  def visitDeleteNode(nodeId: Long);

  def visitUpdateNode(nodeId: Long, addedProps: Map[String, Value], updateProps: Map[String, Value], removeProps: Array[String],
                      addedLabels: Array[String], removedLabels: Array[String]);
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

trait CombinedTransactionCommand {
  def accepts(visitor: CombinedTransactionCommandVisitor);
}