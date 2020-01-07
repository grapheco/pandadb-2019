package cn.pandadb.externalprops

import java.util

import cn.pandadb.util.ConfigUtils._
import cn.pandadb.util.{Configuration, PandaModuleContext}
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.common.{SolrDocument, SolrInputDocument}
import org.neo4j.cypher.internal.runtime.interpreted.{NFLessThan, NFPredicate, _}
import org.neo4j.values.storable.{ArrayValue, Value, Values}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object SolrUtil {
  val idName = "id"
  val labelName = "labels"
  val tik = "id,labels,_version_"
  val arrayName = "Array"
  val dateType = "time"
  val maxRows = 50000000

  def solrDoc2nodeWithProperties(doc: SolrDocument): NodeWithProperties = {
    val props = mutable.Map[String, Value]()
    val id = doc.get(idName)
    val labels = ArrayBuffer[String]()
    if (doc.get(labelName) != null) doc.get(labelName).asInstanceOf[util.ArrayList[String]].foreach(u => labels += u)
    val fieldsName = doc.getFieldNames
    fieldsName.foreach(y => {
      if (tik.indexOf(y) < 0) {
        if (doc.get(y).getClass.getName.contains(arrayName)) {
          val tempArray = ArrayBuffer[AnyRef]()
          doc.get(y).asInstanceOf[util.ArrayList[AnyRef]].foreach(u => tempArray += u)
          if (tempArray.size <= 1) props += y -> Values.of(tempArray.head)
          else props += y -> getValueFromArray(tempArray)
        }
        else props += y -> Values.of(doc.get(y))
      }
    })
    NodeWithProperties(id.toString.toLong, props.toMap, labels)
  }

  def getValueFromArray(value: ArrayBuffer[AnyRef]): Value = {
    val typeObj = value.head
    typeObj match {
      case s1: java.lang.String =>
        val strArr = value.map(_.toString).toArray
        val result = Values.stringArray(strArr: _*)
        result
      case s2: java.lang.Boolean =>
        Values.booleanArray(value.map(_.asInstanceOf[Boolean]).toArray)
      case s3: java.lang.Long =>
        Values.longArray(value.map(_.asInstanceOf[Long]).toArray)
      case s4: java.lang.Byte =>
        Values.byteArray(value.map(_.asInstanceOf[Byte]).toArray)
      case s5: java.lang.Short =>
        Values.shortArray(value.map(_.asInstanceOf[Short]).toArray)
      case s6: java.lang.Integer =>
        Values.intArray(value.map(_.asInstanceOf[Int]).toArray)
      case s7: java.lang.Double =>
        Values.doubleArray(value.map(_.asInstanceOf[Double]).toArray)
      case s8: java.lang.Float =>
        Values.floatArray(value.map(_.asInstanceOf[Float]).toArray)
      case s9: _ => null
    }
  }

}

/**
  * Created by bluejoe on 2019/10/7.
  */
class InSolrPropertyNodeStore(zkUrl: String, collectionName: String) extends CustomPropertyNodeStore {
  //initialize solr connection
  val _solrClient = {
    val client = new CloudSolrClient(zkUrl);
    client.setZkClientTimeout(30000);
    client.setZkConnectTimeout(50000);
    client.setDefaultCollection(collectionName);
    client
  }

  def deleteNodes(docsToBeDeleted: Iterable[Long]): Unit = {
    _solrClient.deleteById(docsToBeDeleted.map(_.toString).toList);
    _solrClient.commit();
  }

  def clearAll(): Unit = {
    _solrClient.deleteByQuery("*:*")
    _solrClient.commit()
  }

  def getRecorderSize: Int = {
    val query = "*:*"
    _solrClient.query(new SolrQuery().setQuery(query)).getResults().getNumFound.toInt
  }

  def addNodes(docsToAdded: Iterable[NodeWithProperties]): Unit = {

    _solrClient.add(docsToAdded.map { x =>
      val doc = new SolrInputDocument();
      x.props.foreach(y => {
        if (Values.isArrayValue(y._2)) {
          y._2.asInstanceOf[ArrayValue].foreach(u => doc.addField(y._1, u.asInstanceOf[Value].asObject()))
        }
        else doc.addField(y._1, y._2.asObject())
      });
      doc.addField(SolrUtil.idName, x.id);
      x.labels.foreach(label => doc.addField(SolrUtil.labelName, label))
      doc
    })

    _solrClient.commit();

  }

  private def predicate2SolrQuery(expr: NFPredicate): String = {
    var q: Option[String] = None
    expr match {
      case expr: NFGreaterThan =>
        val paramValue = expr.value.asInstanceOf[Value].asObject()
        val paramKey = expr.propName
        q = Some(s"$paramKey:{ $paramValue TO * }")
      case expr: NFGreaterThanOrEqual =>
        val paramValue = expr.value.asInstanceOf[Value].asObject()
        val paramKey = expr.propName
        q = Some(s"$paramKey:[ $paramValue TO * ]")
      case expr: NFLessThan =>
        val paramValue = expr.value.asInstanceOf[Value].asObject()
        val paramKey = expr.propName
        q = Some(s"$paramKey:{ * TO $paramValue}")
      case expr: NFLessThanOrEqual =>
        val paramValue = expr.value.asInstanceOf[Value].asObject()
        val paramKey = expr.propName
        q = Some(s"$paramKey:[ * TO $paramValue]")
      case expr: NFEquals =>
        val paramValue = expr.value.asInstanceOf[Value].asObject()
        val paramKey = expr.propName
        q = Some(s"$paramKey:$paramValue")
      case expr: NFNotEquals =>
        val paramValue = expr.value.asInstanceOf[Value].asObject()
        val paramKey = expr.propName
        q = Some(s"-$paramKey:$paramValue")
      case expr: NFNotNull =>
        val paramKey = expr.propName
        q = Some(s"$paramKey:*")
      case expr: NFIsNull =>
        val paramKey = expr.propName
        q = Some(s"-$paramKey:*")
      case expr: NFTrue =>
        q = Some(s"*:*")
      case expr: NFFalse =>
        q = Some(s"-*:*")
      case expr: NFStartsWith =>
        val paramValue = expr.text
        val paramKey = expr.propName
        q = Some(s"$paramKey:$paramValue*")
      case expr: NFEndsWith =>
        val paramValue = expr.text
        val paramKey = expr.propName
        q = Some(s"$paramKey:*$paramValue")
      case expr: NFHasProperty =>
        val paramKey = expr.propName
        q = Some(s"$paramKey:[* TO *]")
      case expr: NFContainsWith =>
        val paramValue = expr.text
        val paramKey = expr.propName
        q = Some(s"$paramKey:*$paramValue*")
      case expr: NFRegexp =>
        val paramValue = expr.text.replace(".", "")
        val paramKey = expr.propName
        q = Some(s"$paramKey:$paramValue")
      case expr: NFAnd =>
        val q1 = predicate2SolrQuery(expr.a)
        val q2 = predicate2SolrQuery(expr.b)
        q = Some(s"($q1 && $q2)")
      case expr: NFOr =>
        val q1 = predicate2SolrQuery(expr.a)
        val q2 = predicate2SolrQuery(expr.b)
        q = Some(s"($q1 || $q2)")
      case expr: NFNot =>
        val q1 = predicate2SolrQuery(expr.a)
        q = if (q1.indexOf("-") >= 0) Some(s"${q1.substring(q1.indexOf("-") + 1)}") else Some(s"-$q1")
      case _ => q = None
    }
    q.get
  }

  override def filterNodes(expr: NFPredicate): Iterable[NodeWithProperties] = {

    var q: Option[String] = None;
    expr match {
      case expr: NFAnd =>
        val q1 = predicate2SolrQuery(expr.a)
        val q2 = predicate2SolrQuery(expr.b)
        q = Some(s"($q1 && $q2)")
      case expr: NFOr =>
        val q1 = predicate2SolrQuery(expr.a)
        val q2 = predicate2SolrQuery(expr.b)
        q = Some(s"($q1 || $q2)")

      case expr: NFNot =>
        val q1 = predicate2SolrQuery(expr.a)
        q = if (q1.indexOf("-") >= 0) Some(s"${q1.substring(q1.indexOf("-") + 1)}") else Some(s"-$q1")

      case _ =>
        val q1 = predicate2SolrQuery(expr)
        q = Some(s"$q1")

    }
    val query = new SolrQuery()
    query.setQuery(q.get)
    val res = new SolrQueryResults(_solrClient, query)
    res.getAllResults()
  }

  override def getNodesByLabel(label: String): Iterable[NodeWithProperties] = {
    val propName = SolrUtil.labelName
    filterNodes(NFContainsWith(propName, label))
  }

  def getNodeBylabelAndFilter(label: String, expr: NFPredicate): Iterable[NodeWithProperties] = {
    val propName = SolrUtil.labelName
    filterNodes(NFAnd(NFContainsWith(propName, label), expr))
  }

  override def getNodeById(id: Long): Option[NodeWithProperties] = {
    val propName = SolrUtil.idName
    filterNodes(NFEquals(propName, Values.of(id))).headOption
  }

  override def close(ctx: PandaModuleContext): Unit = {
    //_solrClient.close()
  }

  override def start(ctx: PandaModuleContext): Unit = {
  }

  override def beginWriteTransaction(): PropertyWriteTransaction = {
    new BufferedExternalPropertyWriteTransaction(this, new InSolrGroupedOpVisitor(true, _solrClient), new InSolrGroupedOpVisitor(false, _solrClient))
  }
}

class InSolrGroupedOpVisitor(isCommit: Boolean, _solrClient: CloudSolrClient) extends GroupedOpVisitor {

  var oldState = mutable.Map[Long, MutableNodeWithProperties]();
  var newState = mutable.Map[Long, MutableNodeWithProperties]();

  def addNodes(docsToAdded: Iterable[NodeWithProperties]): Unit = {

    _solrClient.add(docsToAdded.map { x =>
      val doc = new SolrInputDocument();
      x.props.foreach(y => {
        if (Values.isArrayValue(y._2)) {
          y._2.asInstanceOf[ArrayValue].foreach(u => doc.addField(y._1, u.asInstanceOf[Value].asObject()))
        }
        else doc.addField(y._1, y._2.asObject())
      });
      doc.addField(SolrUtil.idName, x.id);
      x.labels.foreach(label => doc.addField(SolrUtil.labelName, label))
      doc
    })
    _solrClient.commit();

  }

  def getNodeWithPropertiesById(nodeId: Long): NodeWithProperties = {
    val doc = _solrClient.getById(nodeId.toString)
    SolrUtil.solrDoc2nodeWithProperties(doc)
  }

  def deleteNodes(docsToBeDeleted: Iterable[Long]): Unit = {
    _solrClient.deleteById(docsToBeDeleted.map(_.toString).toList);
    _solrClient.commit();
  }

  override def start(ops: GroupedOps): Unit = {

    this.oldState = ops.oldState
    this.newState = ops.newState

  }

  override def end(ops: GroupedOps): Unit = {

  }

  override def visitAddNode(nodeId: Long, props: Map[String, Value], labels: Array[String]): Unit = {
    if (isCommit) addNodes(Iterable(NodeWithProperties(nodeId, props, labels)))
    else visitDeleteNode(nodeId)
  }

  override def visitDeleteNode(nodeId: Long): Unit = {
    if (isCommit) deleteNodes(Iterable(nodeId))
    else {
      val oldNode = oldState.get(nodeId).head
      addNodes(Iterable(NodeWithProperties(nodeId, oldNode.props.toMap, oldNode.labels)))
    }
  }

  def getSolrNodeById(id: Long): SolrDocument = {
    _solrClient.getById(id.toString)
  }

  override def visitUpdateNode(nodeId: Long, addedProps: Map[String, Value],
                               updateProps: Map[String, Value], removeProps: Array[String],
                               addedLabels: Array[String], removedLabels: Array[String]): Unit = {

    if (isCommit) {

      val doc = getSolrNodeById(nodeId)

      val node = SolrUtil.solrDoc2nodeWithProperties(doc)
      val mutiNode = node.mutable()
      mutiNode.props ++= addedProps
      mutiNode.props ++= updateProps
      mutiNode.props --= removeProps
      mutiNode.labels ++= addedLabels
      mutiNode.labels --= removedLabels

      visitAddNode(nodeId, mutiNode.props.toMap, mutiNode.labels.toArray)
    }

    else {
      visitDeleteNode(nodeId)
      val oldNode = oldState.get(nodeId).head
      addNodes(Iterable(NodeWithProperties(nodeId, oldNode.props.toMap, oldNode.labels)))
    }

  }

  override def work(): Unit = {
    val nodeToAdd = ArrayBuffer[NodeWithProperties]()
    val nodeToDelete = ArrayBuffer[Long]()
    if (isCommit) {

      newState.foreach(tle => nodeToAdd += NodeWithProperties(tle._1, tle._2.props.toMap, tle._2.labels))
      oldState.foreach(tle => {
        if (!newState.contains(tle._1)) nodeToDelete += tle._1
      })
    }
    else {

      oldState.foreach(tle => nodeToAdd += NodeWithProperties(tle._1, tle._2.props.toMap, tle._2.labels))
      newState.foreach(tle => {
        if (!oldState.contains(tle._1)) nodeToDelete += tle._1
      })
    }

    if (!nodeToAdd.isEmpty) this.addNodes(nodeToAdd)
    if (!nodeToDelete.isEmpty) this.deleteNodes(nodeToDelete)
  }
}