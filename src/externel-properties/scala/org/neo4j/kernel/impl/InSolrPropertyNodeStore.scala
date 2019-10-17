package org.neo4j.kernel.impl

import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.common.{SolrDocument, SolrInputDocument}
import org.neo4j.cypher.internal.runtime.interpreted.{NFLessThan, NFPredicate, _}

import org.apache.solr.client.solrj.SolrQuery

import org.neo4j.values.storable.{Value, Values}

import scala.collection.JavaConversions._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bluejoe on 2019/10/7.
  */
class InSolrPropertyNodeStore extends CustomPropertyNodeStore {
  var _solrClient: Option[CloudSolrClient] = None;

  override def deleteNodes(docsToBeDeleted: Iterable[Long]): Unit = {
    _solrClient.get.deleteById(docsToBeDeleted.map(_.toString).toList);
    _solrClient.get.commit();

  }

  override def addNodes(docsToAdded: Iterable[CustomPropertyNode]): Unit = {

    _solrClient.get.add(docsToAdded.map { x =>
      val doc = new SolrInputDocument();
      x.fields.foreach(y =>  doc.addField(y._1, y._2.asObject));
     
      doc.addField("id",x.id);
      doc.addField("labels",x.labels.mkString(","));

      doc
    })

    _solrClient.get.commit();


  }

  override def init(): Unit = {
    val zkUrl = "10.0.86.179:2181,10.0.87.45:2181,10.0.87.46:2181"
    val collectionName = "graiphdb"
    val _client = new CloudSolrClient(zkUrl);
    _client.setZkClientTimeout(30000);
    _client.setZkConnectTimeout(50000);
    _client.setDefaultCollection(collectionName);

    _solrClient = Some(_client);



  }
  def predicate2SolrQuery(expr: NFPredicate):String ={
    var q: Option[String] = None
    expr match {
      case expr: NFGreaterThan => {

        val paramValue = expr.value.asInstanceOf[Value].asObject()
        val paramKey = expr.propName
        q = Some(s"$paramKey:{ $paramValue TO * }")

      }
      case expr: NFGreaterThanOrEqual => {

        val paramValue = expr.value.asInstanceOf[Value].asObject()
        val paramKey = expr.propName
        q = Some(s"$paramKey:[ $paramValue TO * ]")

      }

      case expr: NFLessThan => {
        val paramValue = expr.value.asInstanceOf[Value].asObject()
        val paramKey = expr.propName
        q = Some(s"$paramKey:{ * TO $paramValue}")
      }

      case expr: NFLessThanOrEqual => {
        val paramValue = expr.value.asInstanceOf[Value].asObject()
        val paramKey = expr.propName
        q = Some(s"$paramKey:[ * TO $paramValue]")
      }
      case expr: NFEquals => {

        val paramValue = expr.value.asInstanceOf[Value].asObject()
        val paramKey = expr.propName
        q = Some(s"$paramKey:$paramValue")

      }

      case expr: NFNotEquals => {

        val paramValue = expr.value.asInstanceOf[Value].asObject()
        val paramKey = expr.propName
        q = Some(s"-$paramKey:$paramValue")

      }
      case expr: NFNotNull => {


        val paramKey = expr.propName
        q = Some(s"$paramKey:*")

      }

      case expr: NFIsNull => {


        val paramKey = expr.propName
        q = Some(s"-$paramKey:*")

      }
      case expr: NFTrue => {



      }
      case expr: NFFalse => {



      }
      case expr: NFStartsWith => {

        val paramValue = expr.text

        val paramKey = expr.propName
        q = Some(s"$paramKey:$paramValue*")

      }
      case expr: NFEndsWith => {

        val paramValue = expr.text

        val paramKey = expr.propName
        q = Some(s"$paramKey:*$paramValue")

      }

      case expr: NFHasProperty => {



        val paramKey = expr.propName
        q = Some(s"$paramKey:[* TO *]")

      }
      case expr: NFContainsWith => {

        val paramValue = expr.text

        val paramKey = expr.propName
        q = Some(s"$paramKey:*$paramValue*")

      }
      case expr: NFRegexp => {



      }
      case _  => q=None
    }
    q.get

  }
  override def filterNodes(expr: NFPredicate): Iterable[CustomPropertyNode] = {

    val nodeArray = ArrayBuffer[CustomPropertyNode]()
    var q: Option[String] = None;
    expr match {
      case expr:NFAnd => {
        val q1 = predicate2SolrQuery(expr.a)
        val q2 = predicate2SolrQuery(expr.b)
        q = Some(s"$q1 and $q2")
      }
      case expr:NFOr => {
        val q1 = predicate2SolrQuery(expr.a)
        val q2 = predicate2SolrQuery(expr.b)
        q = Some(s"$q1 or $q2")
      }

      case expr:NFNot => {
        val q1 = predicate2SolrQuery(expr.a)
        q = Some(s"-$q1")
      }

      case _  => {
        val q1 = predicate2SolrQuery(expr)
        q = Some(s"$q1")
      }


    }




    _solrClient.get.query(new SolrQuery().setQuery(q.get)).getResults().foreach(
      x => {
        val id = x.get("id")
        val labels = x.get("labels").toString.split(",")

        val tik = "id,labels,_version_"
        val fieldsName = x.getFieldNames
        val fields = for (y <- fieldsName if tik.indexOf(y) < 0) yield (y, Values.of(x.get(y).toString))
        nodeArray += new CustomPropertyNode(id.toString.toLong, fields.toMap, labels)
      }

    )

    nodeArray




  }
  def getCustomPropertyNodeByid(id:Long):SolrDocument={
    _solrClient.get.getById(id.toString)
  }
  def modif2node(node: CustomPropertyNodeModification):CustomPropertyNode={

    val doc = getCustomPropertyNodeByid(node.id)


    val labelsq = doc.get("labels").toString

  
    val labelsTemp = labelsq.substring(labelsq.indexOf('[')+1,labelsq.indexOf(']'))
    var labels = labelsTemp.split(",").toBuffer

    node.labelsAdded.foreach(label => if (!labels.contains(label)) labels +=label)
    node.labelsRemoved.foreach(label => labels -=label)

    val tik = "id,labels,_version_"
    val fieldsName = doc.getFieldNames
    val fields = for (y <- fieldsName if tik.indexOf(y) < 0) yield (y, Values.of(doc.get(y).toString))
    var fieldMap = fields.toMap
    node.fieldsAdded.foreach(fd => fieldMap += fd)
    node.fieldsRemoved.foreach(fd => fieldMap -=fd)
    node.fieldsUpdated.foreach(fd => fieldMap += fd)


    new CustomPropertyNode(node.id, fieldMap, labels)


  }
  override def updateNodes(docsToUpdated: Iterable[CustomPropertyNodeModification]): Unit = {
    docsToUpdated.foreach(node => println(node))
    var docsToAdded = for(doc <- docsToUpdated) yield (modif2node(doc))


    addNodes(docsToAdded)

  }
}
