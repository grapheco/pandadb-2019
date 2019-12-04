package cn.pandadb.externalprops

import cn.pandadb.context.InstanceBoundServiceContext
import org.neo4j.cypher.internal.runtime.interpreted._
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.NumberValue

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by bluejoe on 2019/10/7.
  */
class InMemoryPropertyNodeStoreFactory extends ExternalPropertyStoreFactory {
  override def create(ctx: InstanceBoundServiceContext): CustomPropertyNodeStore = InMemoryPropertyNodeStore;
}


/**
  * used for unit test
  */
object InMemoryPropertyNodeStore extends CustomPropertyNodeStore {
  val nodes = mutable.Map[Long, NodeWithProperties]();

  def filterNodes(expr: NFPredicate): Iterable[NodeWithProperties] = {
    expr match {
      case NFGreaterThan(fieldName: String, value: AnyValue) =>
        nodes.values.filter(x => x.mutable().props.get(fieldName).map(_.asInstanceOf[NumberValue].doubleValue() >
          value.asInstanceOf[NumberValue].doubleValue()).getOrElse(false))

      /*case NFLessThan(fieldName: String, value: AnyValue) =>
        nodes.values.filter(x => x.field(fieldName).map(_.asInstanceOf[NumberValue].doubleValue() <
          value.asInstanceOf[NumberValue].doubleValue()).getOrElse(false))*/
      case NFLessThan(fieldName: String, value: AnyValue) =>
     nodes.values.filter(x => x.mutable().props.get(fieldName).map(_.asInstanceOf[NumberValue].doubleValue() <
       value.asInstanceOf[NumberValue].doubleValue()).getOrElse(false))

      case NFLessThanOrEqual(fieldName: String, value: AnyValue) =>
        nodes.values.filter(x => x.mutable().props.get(fieldName).map(_.asInstanceOf[NumberValue].doubleValue() <=
          value.asInstanceOf[NumberValue].doubleValue()).getOrElse(false))


      case NFGreaterThanOrEqual(fieldName: String, value: AnyValue) =>
        nodes.values.filter(x => x.mutable().props.get(fieldName).map(_.asInstanceOf[NumberValue].doubleValue() >=
          value.asInstanceOf[NumberValue].doubleValue()).getOrElse(false))


      case NFEquals(fieldName: String, value: AnyValue) =>
        nodes.values.filter(x => x.mutable().props.get(fieldName).map(_.asInstanceOf[NumberValue].doubleValue() ==
          value.asInstanceOf[NumberValue].doubleValue()).getOrElse(false))

    }
  }

  def deleteNodes(docsToBeDeleted: Iterable[Long]): Unit = {
    nodes --= docsToBeDeleted
  }

  def addNodes(docsToAdded: Iterable[NodeWithProperties]): Unit = {
    nodes ++= docsToAdded.map(x => x.id -> x)
  }

  def updateNodes(nodeId: Long, addedProps: Map[String, Value],
                           updateProps: Map[String, Value], removeProps: Array[String],
                           addedLabels: Array[String], removedLabels: Array[String]): Unit = {

      val n: MutableNodeWithProperties = nodes(nodeId).mutable()
      if (addedProps != null && addedProps.size>0) {
        n.props ++= addedProps
      }
      if (updateProps != null && updateProps.size>0) {
        n.props ++= updateProps
      }
      if (removeProps != null && removeProps.size>0) {
        removeProps.foreach(f => n.props -= f)
      }
      if (addedLabels != null && addedLabels.size>0) {
        n.labels ++= addedLabels
       // nodes(nodeId).labels = nodes(nodeId).labels.toSet
      }
      if (removedLabels != null && removedLabels.size>0) {
        //val tmpLabels = nodes(nodeId).labels.toSet
        n.labels  --= removedLabels
      }
    deleteNodes(Iterable(nodeId))
    addNodes(Iterable(NodeWithProperties(nodeId, n.props.toMap, n.labels)))


  }

  override def getNodesByLabel(label: String): Iterable[NodeWithProperties] = {
    val res = mutable.ArrayBuffer[NodeWithProperties]()
    nodes.map(n => {
      if (n._2.labels.toArray.contains(label)) {
        res.append(n._2)
      }

    })
    res
  }

  override def getNodeById(id: Long): Option[NodeWithProperties] = {
    nodes.get(id)
  }

  override def start(ctx: InstanceBoundServiceContext): Unit = {
    nodes.clear()
  }

  override def stop(ctx: InstanceBoundServiceContext): Unit = {
    nodes.clear()
  }

  override def beginWriteTransaction(): PropertyWriteTransaction = {
    new BufferedExternalPropertyWriteTransaction(this, new InMemoryGroupedOpVisitor(true, nodes), new InMemoryGroupedOpVisitor(false, nodes))
  }
}
class InMemoryGroupedOpVisitor(isCommit: Boolean, nodes: mutable.Map[Long, NodeWithProperties]) extends GroupedOpVisitor {

  var oldState = mutable.Map[Long, MutableNodeWithProperties]();
  var newState = mutable.Map[Long, MutableNodeWithProperties]();


  override def start(ops: GroupedOps): Unit = {


    this.oldState = ops.oldState
    this.newState = ops.newState

  }

  override def end(ops: GroupedOps): Unit = {

   // this.oldState.clear()
   // this.newState.clear()

  }

  override def visitAddNode(nodeId: Long, props: Map[String, Value], labels: Array[String]): Unit = {
    if (isCommit) InMemoryPropertyNodeStore.addNodes(Iterable(NodeWithProperties(nodeId, props, labels)))
    else {
      InMemoryPropertyNodeStore.deleteNodes(Iterable(nodeId))
    }



  }

  override def visitDeleteNode(nodeId: Long): Unit = {
    if (isCommit) InMemoryPropertyNodeStore.deleteNodes(Iterable(nodeId))
    else {

      val oldNode = oldState.get(nodeId).head
      InMemoryPropertyNodeStore.addNodes(Iterable(NodeWithProperties(nodeId, oldNode.props.toMap, oldNode.labels)))

    }
  }

  override def visitUpdateNode(nodeId: Long, addedProps: Map[String, Value],
                               updateProps: Map[String, Value], removeProps: Array[String],
                               addedLabels: Array[String], removedLabels: Array[String]): Unit = {
      if (isCommit) InMemoryPropertyNodeStore.updateNodes(nodeId: Long, addedProps: Map[String, Value],
      updateProps: Map[String, Value], removeProps: Array[String],
      addedLabels: Array[String], removedLabels: Array[String])
      else {

        val oldNode = oldState.get(nodeId).head
        InMemoryPropertyNodeStore.addNodes(Iterable(NodeWithProperties(nodeId, oldNode.props.toMap, oldNode.labels)))

      }
  }

  override def work(): Unit = {

    val nodeToAdd = ArrayBuffer[NodeWithProperties]()
    val nodeToDelete = ArrayBuffer[Long]()
    if (isCommit) {

      newState.foreach(tle => nodeToAdd += NodeWithProperties(tle._1, tle._2.props.toMap, tle._2.labels))
      oldState.foreach(tle => {if (!newState.contains(tle._1)) nodeToDelete += tle._1})
    }
    else {

      oldState.foreach(tle => nodeToAdd += NodeWithProperties(tle._1, tle._2.props.toMap, tle._2.labels))
      newState.foreach(tle => {if (!oldState.contains(tle._1)) nodeToDelete += tle._1})
    }

    InMemoryPropertyNodeStore.addNodes(nodeToAdd)
    InMemoryPropertyNodeStore.deleteNodes(nodeToDelete)

  }
}