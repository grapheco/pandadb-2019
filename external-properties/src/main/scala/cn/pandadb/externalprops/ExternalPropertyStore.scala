package cn.pandadb.externalprops

import java.util.{Map => JMap}

import cn.pandadb.context.{InstanceBoundService, InstanceBoundServiceContext}
import cn.pandadb.util.PandaException
import org.neo4j.cypher.internal.runtime.interpreted.NFPredicate
import org.neo4j.values.storable.{Value, Values}
import org.neo4j.values.virtual.{NodeValue, VirtualValues}

import scala.collection.JavaConversions
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
  def deleteNode(nodeIds: Array[Long]);

  def addNode(nodes: Array[NodeWithProperties]);

  def addProperty(nodeId: Long, properties: Map[String, Value]): Unit;

  def addProperty(nodeId: Long, properties: JMap[String, Value]): Unit =
    addProperty(nodeId, JavaConversions.mapAsScalaMap(properties).toMap);

  def removeProperty(nodeId: Long, propertyNames: Array[String]);

  def updateProperty(nodeId: Long, properties: Map[String, Value]): Unit;

  def updateProperty(nodeId: Long, properties: JMap[String, Value]): Unit =
    updateProperty(nodeId, JavaConversions.mapAsScalaMap(properties).toMap);

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
  val buffer = ArrayBuffer[BufferCommand]();

  override def deleteNode(nodeIds: Array[Long]): Unit =
    buffer ++= nodeIds.map(DeleteNodeCommand(_))

  override def updateProperty(nodeId: Long, properties: Map[String, Value]): Unit =
    buffer ++= properties.map(prop => UpdatePropertyCommand(nodeId, prop._1, prop._2))

  override def addNode(nodes: Array[NodeWithProperties]): Unit =
    buffer ++= nodes.map(AddNodeCommand(_))

  override def addProperty(nodeId: Long, properties: Map[String, Value]): Unit =
    buffer ++= properties.map(prop => AddPropertyCommand(nodeId, prop._1, prop._2))

  override def removeProperty(nodeId: Long, propertyNames: Array[String]): Unit =
    buffer ++= propertyNames.map(RemovePropertyCommand(nodeId, _))

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

  def internalCommit(commands: Array[BufferCommand]);

  def internalRollback(commands: Array[BufferCommand]);

  def internalCheck(commands: Array[BufferCommand]): Option[Throwable];
}

trait BufferCommand {

}

case class DeleteNodeCommand(nodeId: Long) extends BufferCommand {

}

case class AddNodeCommand(node: NodeWithProperties) extends BufferCommand {

}

case class UpdatePropertyCommand(nodeId: Long, key: String, value: Value) extends BufferCommand {

}

case class RemovePropertyCommand(nodeId: Long, key: String) extends BufferCommand {

}

case class AddPropertyCommand(nodeId: Long, key: String, value: Value) extends BufferCommand {

}

