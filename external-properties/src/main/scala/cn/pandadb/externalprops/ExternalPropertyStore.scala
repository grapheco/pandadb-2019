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

trait CustomPropertyNodeReader {
  def filterNodes(expr: NFPredicate): Iterable[NodeWithProperties];

  def getNodesByLabel(label: String): Iterable[NodeWithProperties];

  def getNodeBylabelAndfilter(label: String, expr: NFPredicate): Iterable[NodeWithProperties];

  def getNodeById(id: Long): Option[NodeWithProperties];
}

trait CustomPropertyNodeStore extends InstanceBoundService with CustomPropertyNodeReader {
  def beginWriteTransaction(): PropertyWriteTransaction;
}

trait PropertyWriter {
  def deleteNode(nodeId: Long);

  def addNode(nodeId: Long);

  def addProperty(nodeId: Long, key: String, value: Value): Unit;

  def removeProperty(nodeId: Long, key: String);

  def updateProperty(nodeId: Long, key: String, value: Value): Unit;

  def addLabel(nodeId: Long, label: String): Unit;

  def removeLabel(nodeId: Long, label: String): Unit;
}

trait PropertyReaderWithinTransaction {
  def getNodeLabels(nodeId: Long): Array[String];

  def getPropertyValue(nodeId: Long, key: String): Option[Value];
}

trait PropertyWriteTransaction extends PropertyWriter with PropertyReaderWithinTransaction {
  @throws[FailedToCommitTransaction]
  def commit(): mutable.Undoable;

  @throws[FailedToRollbackTransaction]
  def rollback(): Unit;

  def close(): Unit;
}

class FailedToCommitTransaction(tx: PropertyWriteTransaction, cause: Throwable)
  extends PandaException("failed to commit transaction: $tx") {

}

class FailedToRollbackTransaction(tx: PropertyWriteTransaction, cause: Throwable)
  extends PandaException("failed to roll back transaction: $tx") {

}

case class NodeWithProperties(id: Long, props: Map[String, Value], labels: Iterable[String]) {
  def toNeo4jNodeValue(): NodeValue = {
    VirtualValues.nodeValue(id,
      Values.stringArray(labels.toArray: _*),
      VirtualValues.map(props.keys.toArray, props.values.toArray))
  }

  def mutable(): MutableNodeWithProperties = {
    val m = MutableNodeWithProperties(id);
    m.props ++= props;
    m.labels ++= labels;
    m;
  }
}

case class MutableNodeWithProperties(id: Long) {
  val props = mutable.Map[String, Value]();
  val labels = ArrayBuffer[String]();
}

class BufferedExternalPropertyWriteTransaction(
                                                   nodeReader: CustomPropertyNodeReader,
                                                   commitPerformer: GroupedOpVisitor,
                                                   undoPerformer: GroupedOpVisitor)
  extends PropertyWriteTransaction {
  val bufferedOps = ArrayBuffer[BufferedPropertyOp]();
  val oldState = mutable.Map[Long, MutableNodeWithProperties]();
  val newState = mutable.Map[Long, MutableNodeWithProperties]();
  private var isCommited = false
  override def deleteNode(nodeId: Long): Unit = {
    getPopulatedNode(nodeId)
    //bufferedOps += BufferedDeleteNodeOp(nodeId)
    newState.remove(nodeId)
  }

  //get node related info when required
  private def getPopulatedNode(nodeId: Long): MutableNodeWithProperties = {

    if (isNodeExitInNewState(nodeId)) newState.get(nodeId).get
    else {
      if (!isNodeExitInOldState(nodeId)) {
        val state = nodeReader.getNodeById(nodeId).get //get from database,if failue throw exception no such node
        oldState += nodeId -> state.mutable()
        newState += nodeId -> state.mutable()
        newState.get(nodeId).get
      }
      else null  //throw exception ,node deleted
    }

  }
  private def isNodeExitInNewState(nodeId: Long): Boolean = {
    newState.contains(nodeId)
  }
  private def isNodeExitInOldState(nodeId: Long): Boolean = {
    oldState.contains(nodeId)
  }

  override def addNode(nodeId: Long): Unit = {
    //bufferedOps += BufferedAddNodeOp(nodeId)
    if (isNodeExitInNewState(nodeId)) null //throw exception node already exist
    else newState += nodeId -> MutableNodeWithProperties(nodeId)
  }

  override def addProperty(nodeId: Long, key: String, value: Value): Unit = {
    //bufferedOps += BufferedAddPropertyOp(nodeId, key, value)
    val state = getPopulatedNode(nodeId)
    state.props += (key -> value);
    newState += nodeId -> state
  }

  override def removeProperty(nodeId: Long, key: String): Unit = {
    //bufferedOps += BufferedRemovePropertyOp(nodeId, key)
    val state = getPopulatedNode(nodeId)
    state.props -= key;
    newState += nodeId -> state
  }

  override def updateProperty(nodeId: Long, key: String, value: Value): Unit = {
    //bufferedOps += BufferedUpdatePropertyOp(nodeId, key, value)
    val state = getPopulatedNode(nodeId)
    state.props += (key -> value);
    newState += nodeId -> state
  }

  override def addLabel(nodeId: Long, label: String): Unit = {
    //bufferedOps += BufferedAddLabelOp(nodeId, label)
    val state = getPopulatedNode(nodeId)
    state.labels += label
    newState += nodeId -> state
  }

  override def removeLabel(nodeId: Long, label: String): Unit = {
    //bufferedOps += BufferedRemoveLabelOp(nodeId, label)
    //getPopulatedNode(nodeId).labels -= label
    val state = getPopulatedNode(nodeId)
    state.labels -= label
    newState += nodeId -> state
  }

  def getNodeLabels(nodeId: Long): Array[String] = {
    getPopulatedNode(nodeId).labels.toArray
  }

  def getPropertyValue(nodeId: Long, key: String): Option[Value] = {
    getPopulatedNode(nodeId).props.get(key)
  }

  @throws[FailedToCommitTransaction]
  def commit(): mutable.Undoable = {
    val ops: GroupedOps = GroupedOps(bufferedOps.toArray)
    ops.newState = this.newState
    ops.oldState = this.oldState
    if (!isCommited) {
      doPerformerWork(ops, commitPerformer)
      isCommited = true
    }
    else {
      Unit //throw exception,cannot commit twice
    }
    new mutable.Undoable() {
      def undo(): Unit = {
        if (isCommited) {
          doPerformerWork(ops, undoPerformer)
          isCommited = false
        }
      }
    }
  }

  @throws[FailedToRollbackTransaction]
  def rollback(): Unit = {
  }

  def close(): Unit = {
    bufferedOps.clear()
    newState.clear()
    oldState.clear()
  }

  private def doPerformerWork(ops: GroupedOps, performer: GroupedOpVisitor): Unit = {
    performer.start(ops)
    performer.work()
    performer.end(ops)
  }
}

/**
  * buffer based implementation of ExternalPropertyWriteTransaction
  * this is a template class which should be derived
  */

case class GroupedOps(ops: Array[BufferedPropertyOp]) {
  //commands-->combined
  val addedNodes = mutable.Map[Long, GroupedAddNodeOp]();
  val updatedNodes = mutable.Map[Long, GroupedUpdateNodeOp]();
  val deleteNodes = ArrayBuffer[GroupedDeleteNodeOp]();
  var oldState = mutable.Map[Long, MutableNodeWithProperties]();
  var newState = mutable.Map[Long, MutableNodeWithProperties]();

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
  def work();
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
    visitor.visitUpdateNode(nodeId, addedProps.toMap, updatedProps.toMap,
      removedProps.toArray, addedLabels.toArray, removedLabels.toArray)
  }
}

case class GroupedDeleteNodeOp(nodeId: Long) extends GroupedOp {
  def accepts(visitor: GroupedOpVisitor): Unit = {
    visitor.visitDeleteNode(nodeId)
  }
}