package cn.pandadb.externalprops

import cn.pandadb.context.{InstanceBoundService, InstanceBoundServiceContext}
import cn.pandadb.util.PandaException
import org.neo4j.cypher.internal.runtime.interpreted.NFPredicate
import org.neo4j.values.storable.{Value, Values}
import org.neo4j.values.virtual.{NodeValue, VirtualValues}

import scala.collection.mutable
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
  val bufferedOps = ArrayBuffer[BufferedPropertyOp]();

  override def deleteNode(nodeId: Long): Unit = bufferedOps += BufferedDeleteNodeOp(nodeId)

  override def addNode(nodeId: Long): Unit = bufferedOps += BufferedAddNodeOp(nodeId)

  override def addProperty(nodeId: Long, key: String, value: Value): Unit = bufferedOps += BufferedAddPropertyOp(nodeId, key, value)

  override def removeProperty(nodeId: Long, key: String): Unit = bufferedOps += BufferedRemovePropertyOp(nodeId, key)

  override def updateProperty(nodeId: Long, key: String, value: Value): Unit = bufferedOps += BufferedUpdatePropertyOp(nodeId, key, value)

  override def addLabel(nodeId: Long, label: String): Unit = bufferedOps += BufferedAddLabelOp(nodeId, label)

  override def removeLabel(nodeId: Long, label: String): Unit = bufferedOps += BufferedRemoveLabelOp(nodeId, label)

  override def startWriteTransaction(): PropertyWriteTransaction =
    new VisitorPreparedTransaction(GroupedOps(bufferedOps.toArray), commitPerformer(), rollbackPerformer())

  def commitPerformer(): GroupedOpVisitor;

  def rollbackPerformer(): GroupedOpVisitor;
}

class VisitorPreparedTransaction(
                                  ops: GroupedOps,
                                  commitPerformer: GroupedOpVisitor,
                                  rollbackPerformer: GroupedOpVisitor)
  extends PropertyWriteTransaction {

  private def doPerformerWork(performer: GroupedOpVisitor): Unit = {
    performer.start(ops)
    ops.accepts(performer)
    performer.end(ops)
  }

  @throws[FailedToCommitTransaction]
  override def commit(): Unit = {
    doPerformerWork(commitPerformer)
  }

  @throws[FailedToRollbackTransaction]
  override def rollback(): Unit = {
    doPerformerWork(rollbackPerformer)
  }
}

case class GroupedOps(ops: Array[BufferedPropertyOp]) {

  //commands-->combined
  val addedNodes = mutable.Map[Long, GroupedAddNodeOp]();
  val updatedNodes = mutable.Map[Long, GroupedUpdateNodeOp]();
  val deleteNodes = ArrayBuffer[GroupedDeleteNodeOp]();

  ops.foreach {
    _ match {
      case BufferedAddNodeOp(nodeId: Long) => addedNodes += nodeId -> GroupedAddNodeOp(nodeId)
      case BufferedDeleteNodeOp(nodeId: Long) => deleteNodes += GroupedDeleteNodeOp(nodeId)
      case BufferedDeleteNodeOp(nodeId: Long) =>
        addedNodes -= nodeId
        updatedNodes -= nodeId
        deleteNodes += GroupedDeleteNodeOp(nodeId)
      case BufferedUpdatePropertyOp(nodeId: Long, key: String, value: Value) =>
        if (addedNodes.isDefinedAt(nodeId)) {
          addedNodes(nodeId).addedProps += key -> value
        }
        if (updatedNodes.isDefinedAt(nodeId)) {
          updatedNodes(nodeId).updatedProps += key -> value
        }
      case BufferedRemovePropertyOp(nodeId: Long, key: String) =>
        if (addedNodes.isDefinedAt(nodeId)) {
          addedNodes(nodeId).addedProps -= key
        }
        if (updatedNodes.isDefinedAt(nodeId)) {
          updatedNodes(nodeId).updatedProps -= key
        }
      case BufferedAddPropertyOp(nodeId: Long, key: String, value: Value) =>
        if (addedNodes.isDefinedAt(nodeId)) {
          addedNodes(nodeId).addedProps += key -> value
        }
        if (updatedNodes.isDefinedAt(nodeId)) {
          updatedNodes(nodeId).addedProps += key -> value
        }
      case BufferedAddLabelOp(nodeId: Long, label: String) =>
        if (addedNodes.isDefinedAt(nodeId)) {
          addedNodes(nodeId).addedLabels += label
        }
        if (updatedNodes.isDefinedAt(nodeId)) {
          updatedNodes(nodeId).addedLabels += label
        }
      case BufferedRemoveLabelOp(nodeId: Long, label: String) =>
        if (addedNodes.isDefinedAt(nodeId)) {
          addedNodes(nodeId).addedLabels -= label
        }
        if (updatedNodes.isDefinedAt(nodeId)) {
          updatedNodes(nodeId).removedLabels += label
        }
    }
  }

  def accepts(visitor: GroupedOpVisitor): Unit = {
    addedNodes.values.foreach(_.accepts(visitor))
    updatedNodes.values.foreach(_.accepts(visitor))
    deleteNodes.foreach(_.accepts(visitor))
  }
}

trait GroupedOpVisitor {
  def start(ops: GroupedOps);

  def end(ops: GroupedOps);

  def visitAddNode(nodeId: Long, props: Map[String, Value], labels: Array[String]);

  def visitDeleteNode(nodeId: Long);

  def visitUpdateNode(nodeId: Long, addedProps: Map[String, Value], updateProps: Map[String, Value], removeProps: Array[String],
                      addedLabels: Array[String], removedLabels: Array[String]);
}

/**
  * buffered operation within a prepared transaction
  */
trait BufferedPropertyOp {

}

case class BufferedDeleteNodeOp(nodeId: Long) extends BufferedPropertyOp {

}

case class BufferedAddNodeOp(nodeId: Long) extends BufferedPropertyOp {

}

case class BufferedUpdatePropertyOp(nodeId: Long, key: String, value: Value) extends BufferedPropertyOp {

}

case class BufferedRemovePropertyOp(nodeId: Long, key: String) extends BufferedPropertyOp {

}

case class BufferedAddPropertyOp(nodeId: Long, key: String, value: Value) extends BufferedPropertyOp {

}

case class BufferedAddLabelOp(nodeId: Long, label: String) extends BufferedPropertyOp {

}

case class BufferedRemoveLabelOp(nodeId: Long, label: String) extends BufferedPropertyOp {

}

/**
  * grouped operation to be committed
  */
trait GroupedOp {
  def accepts(visitor: GroupedOpVisitor): Unit;
}

case class GroupedAddNodeOp(nodeId: Long) extends GroupedOp {
  val addedProps = mutable.Map[String, Value]();
  val addedLabels = mutable.Set[String]();

  def accepts(visitor: GroupedOpVisitor): Unit = {
    visitor.visitAddNode(nodeId, addedProps.toMap, addedLabels.toArray)
  }
}

case class GroupedUpdateNodeOp(nodeId: Long) extends GroupedOp {
  val addedProps = mutable.Map[String, Value]();
  val updatedProps = mutable.Map[String, Value]();
  val removedProps = mutable.Set[String]();
  val addedLabels = mutable.Set[String]();
  val removedLabels = mutable.Set[String]();

  def accepts(visitor: GroupedOpVisitor): Unit = {
    visitor.visitUpdateNode(nodeId, addedProps.toMap, updatedProps.toMap, removedProps.toArray, addedLabels.toArray, removedLabels.toArray)
  }
}

case class GroupedDeleteNodeOp(nodeId: Long) extends GroupedOp {
  def accepts(visitor: GroupedOpVisitor): Unit = {
    visitor.visitDeleteNode(nodeId)
  }
}