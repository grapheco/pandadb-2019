package cn.pandadb.externalprops

import java.util

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import org.neo4j.cypher.internal.runtime.interpreted.{NFLessThan, NFPredicate, _}
import org.neo4j.values.storable._
import cn.pandadb.util.ConfigUtils._
import cn.pandadb.util.{Configuration, PandaModuleContext}
import com.alibaba.fastjson.{JSONArray, JSONObject}
import com.fasterxml.jackson.databind.ObjectMapper
import com.sun.jdi.IntegerValue
import org.apache.http.HttpHost
import org.elasticsearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.elasticsearch.action.admin.indices.create.{CreateIndexRequest, CreateIndexResponse}
import org.elasticsearch.action.index.{IndexRequest, IndexResponse}
import org.elasticsearch.common.xcontent.{XContentBuilder, XContentFactory, XContentType}
import org.elasticsearch.action.get.{GetRequest, GetResponse}
import org.elasticsearch.action.update.{UpdateRequest, UpdateResponse}
import org.elasticsearch.action.delete.{DeleteRequest, DeleteResponse}
import org.elasticsearch.common.Strings
import org.elasticsearch.search.fetch.subphase.FetchSourceContext
import org.elasticsearch.action.search.{SearchRequest, SearchResponse}
import org.elasticsearch.index.query.{QueryBuilder, QueryBuilders}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.index.reindex.{BulkByScrollResponse, DeleteByQueryRequest}
import org.elasticsearch.action.support.WriteRequest
import org.elasticsearch.script.ScriptType
import org.elasticsearch.script.mustache.{SearchTemplateRequest, SearchTemplateResponse}


object EsUtil {
  val idName = "id"
  val labelName = "labels"
  val tik = "id,labels,_version_"
  val arrayName = "Array"
  val dateType = "time"

  def getValueFromArray(value: Array[AnyRef]): Value = {
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
      case _ => null
    }
  }

  def neo4jValueToScala(value: Value): Any = {
    value match {
      case v: IntegralValue => v.asInstanceOf[IntegralValue].longValue()
      case v: IntegralArray =>
        v.asInstanceOf[IntegralArray].iterator().map(v2 => v2.asInstanceOf[IntegralValue].longValue()).toArray
      case v: FloatingPointValue => v.asInstanceOf[FloatingPointValue].doubleValue()
      case v: FloatingPointArray =>
        v.asInstanceOf[FloatingPointArray].iterator().map(v2 => v2.asInstanceOf[FloatingPointValue].doubleValue()).toArray
      case v: TextValue => v.asInstanceOf[TextValue].stringValue()
      case v: TextArray =>
        v.asInstanceOf[TextArray].iterator().map(v2 => v2.asInstanceOf[TextValue].stringValue()).toArray
      case v: BooleanValue => v.asInstanceOf[BooleanValue].booleanValue()
      case v: BooleanArray =>
        v.asInstanceOf[BooleanArray].iterator().map(v2 => v2.asInstanceOf[BooleanValue].booleanValue()).toArray
      case v => v.asObject()
    }
  }

  def sourceMapToNodeWithProperties(doc: Map[String, Object]): NodeWithProperties = {
    val props = mutable.Map[String, Value]()
    var id: Long = -1
    if (doc.contains(idName)) {
      id = doc.get(idName).get.asInstanceOf[Int].toLong
    }
    val labels = ArrayBuffer[String]()
    if (doc.contains(labelName)) doc.get(labelName).get.asInstanceOf[util.ArrayList[String]].foreach(u => labels += u)
    doc.map(field =>
      if (!field._1.equals(idName) && !field._1.equals(labelName) ) {
        if (field._2.isInstanceOf[util.ArrayList[Object]]) {
          props(field._1) = getValueFromArray(field._2.asInstanceOf[util.ArrayList[Object]].toArray())
        }
        else props(field._1) = Values.of(field._2)
      }
    )

    NodeWithProperties(id.toString.toLong, props.toMap, labels)
  }

  def createClient(host: String, port: Int, schema: String = "http"): RestHighLevelClient = {
    val httpHost = new HttpHost(host, port, schema)
    val builder = RestClient.builder(httpHost)
    val client = new RestHighLevelClient(builder)
    client
  }

  def createIndex(client: RestHighLevelClient, indexName: String): Boolean = {
    val indexRequest: CreateIndexRequest = new CreateIndexRequest(indexName)
    val indexResponse: CreateIndexResponse = client.indices().create(indexRequest)
    indexResponse.isAcknowledged
  }

  def addData(client: RestHighLevelClient, indexName: String, typeName: String, id: String, builder: XContentBuilder): String = {
    val indexRequest: IndexRequest = new IndexRequest(indexName, typeName, id)
    indexRequest.source(builder)
    indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
    val indexResponse: IndexResponse = client.index(indexRequest)
    indexResponse.getId
  }

  def updateData(client: RestHighLevelClient, indexName: String, typeName: String, id: String, data: JSONObject): String = {
    val request = new UpdateRequest(indexName, typeName, id)
    request.doc(data.toString, XContentType.JSON)
    request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
    val response: UpdateResponse = client.update(request, RequestOptions.DEFAULT)
    response.toString
  }

  def deleteData(client: RestHighLevelClient, indexName: String, typeName: String, id: String): String = {
    val request: DeleteRequest = new DeleteRequest(indexName, typeName, id)
    request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
    val response: DeleteResponse = client.delete(request, RequestOptions.DEFAULT)
    response.toString
  }

  def getData(client: RestHighLevelClient, indexName: String, typeName: String, id: String): mutable.Map[String, Object] = {
    val request: GetRequest = new GetRequest(indexName, typeName, id)
    val includes = Strings.EMPTY_ARRAY
    val excludes = Strings.EMPTY_ARRAY
    val fetchSourceContext = new FetchSourceContext(true, includes, excludes)
    request.fetchSourceContext(fetchSourceContext)
    val response = client.get(request, RequestOptions.DEFAULT)
    response.getSource
  }

  def getAllSize(client: RestHighLevelClient, indexName: String, typeName: String): Long = {
    val searchRequest: SearchRequest = new SearchRequest();
    searchRequest.indices(indexName)
    searchRequest.types(typeName)
    val searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(QueryBuilders.matchAllQuery());
    searchSourceBuilder.fetchSource(false)
    searchRequest.source(searchSourceBuilder);
    val searchResponse = client.search(searchRequest, RequestOptions.DEFAULT)
    searchResponse.getHits.totalHits
  }

  def clearAllData(client: RestHighLevelClient, indexName: String, typeName: String): Long = {
    val request: DeleteByQueryRequest = new DeleteByQueryRequest()
    request.indices(indexName)
    request.types(typeName)
    request.setQuery(QueryBuilders.matchAllQuery())
    request.setRefresh(true)
    val response: BulkByScrollResponse = client.deleteByQuery(request, RequestOptions.DEFAULT)
    response.getDeleted
  }

  def searchTemplate(client: RestHighLevelClient, indexName: String, typeName: String,
            queryScriptTempl: String, scriptParams: Map[String, Object]): SearchTemplateResponse = {
    val searchRequest = new SearchRequest()
    searchRequest.indices(indexName)
    searchRequest.types(typeName)
    val request = new SearchTemplateRequest
    request.setRequest(searchRequest)
    request.setScriptType(ScriptType.INLINE)
    request.setScript(queryScriptTempl)
    request.setScriptParams(Map[String, Object]())
    val response = client.searchTemplate(request, RequestOptions.DEFAULT)
    response
  }

  def search(client: RestHighLevelClient, indexName: String, typeName: String, queryBuilder: QueryBuilder): SearchResponse = {
    val searchRequest = new SearchRequest();
    searchRequest.indices(indexName)
    searchRequest.types(typeName)
    val searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(queryBuilder);
    searchRequest.source(searchSourceBuilder);
    client.search(searchRequest, RequestOptions.DEFAULT)
  }
}


class InElasticSearchPropertyNodeStore(host: String, port: Int, indexName: String, typeName: String) extends CustomPropertyNodeStore {
  //initialize solr connection
  val esClient = EsUtil.createClient(host, port)

  def deleteNodes(docsToBeDeleted: Iterable[Long]): Unit = {
    docsToBeDeleted.foreach(node => EsUtil.deleteData(esClient, indexName, typeName, node.toString))
  }

  def clearAll(): Unit = {
    EsUtil.clearAllData(esClient, indexName, typeName)
  }

  def getRecorderSize(): Long = {
    EsUtil.getAllSize(esClient, indexName, typeName)
  }

  private def predicate2EsQuery(expr: NFPredicate): QueryBuilder = {
    expr match {
      case expr: NFGreaterThan =>
        val paramValue = expr.value.asInstanceOf[Value].asObject()
        val paramKey = expr.propName
        QueryBuilders.rangeQuery(paramKey).gt(paramValue)
      case expr: NFGreaterThanOrEqual =>
        val paramValue = expr.value.asInstanceOf[Value].asObject()
        val paramKey = expr.propName
        QueryBuilders.rangeQuery(paramKey).gte(paramValue)
      case expr: NFLessThan =>
        val paramValue = expr.value.asInstanceOf[Value].asObject()
        val paramKey = expr.propName
        QueryBuilders.rangeQuery(paramKey).lt(paramValue)
      case expr: NFLessThanOrEqual =>
        val paramValue = expr.value.asInstanceOf[Value].asObject()
        val paramKey = expr.propName
        QueryBuilders.rangeQuery(paramKey).lte(paramValue)
      case expr: NFEquals =>
        val paramValue = expr.value.asInstanceOf[Value].asObject()
        val paramKey = expr.propName
        QueryBuilders.termQuery(paramKey, paramValue)
      case expr: NFNotEquals =>
        val paramValue = expr.value.asInstanceOf[Value].asObject()
        val paramKey = expr.propName
        QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery(paramKey, paramValue))
      case expr: NFNotNull =>
        val paramKey = expr.propName
        QueryBuilders.existsQuery(paramKey)
      case expr: NFIsNull =>
        val paramKey = expr.propName
        QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(paramKey))
      case expr: NFTrue =>
        QueryBuilders.matchAllQuery()
      case expr: NFFalse =>
        QueryBuilders.boolQuery().mustNot(QueryBuilders.matchAllQuery())
      case expr: NFStartsWith =>
        val paramValue = expr.text
        val paramKey = expr.propName
        QueryBuilders.prefixQuery(paramKey, paramValue)
      case expr: NFEndsWith =>
        val paramValue = expr.text
        val paramKey = expr.propName
        QueryBuilders.regexpQuery(paramKey, ".*" + paramValue)
      case expr: NFHasProperty =>
        val paramKey = expr.propName
        QueryBuilders.existsQuery(paramKey)
      case expr: NFContainsWith =>
        val paramValue = expr.text
        val paramKey = expr.propName
        QueryBuilders.regexpQuery(paramKey, ".*" + paramValue + ".*")
      case expr: NFRegexp =>
        val paramValue = expr.text.replace(".", "")
        val paramKey = expr.propName
        QueryBuilders.regexpQuery(paramKey, paramValue)
      case expr: NFAnd =>
        val q1 = predicate2EsQuery(expr.a)
        val q2 = predicate2EsQuery(expr.b)
        QueryBuilders.boolQuery().must(q1).must(q2)
      case expr: NFOr =>
        val q1 = predicate2EsQuery(expr.a)
        val q2 = predicate2EsQuery(expr.b)
        QueryBuilders.boolQuery().should(q1).should(q2)
      case expr: NFNot =>
        val q1 = predicate2EsQuery(expr.a)
        QueryBuilders.boolQuery().mustNot(q1)
    }
  }

  override def filterNodes(expr: NFPredicate): Iterable[NodeWithProperties] = {
    val q = predicate2EsQuery(expr)
    val res = EsUtil.search(esClient, indexName, typeName, q).getHits
    res.getHits.map(h => EsUtil.sourceMapToNodeWithProperties(h.getSourceAsMap.toMap))
  }

  override def getNodesByLabel(label: String): Iterable[NodeWithProperties] = {
    val propName = EsUtil.labelName
    filterNodes(NFContainsWith(propName, label))
  }

  def getNodeBylabelAndFilter(label: String, expr: NFPredicate): Iterable[NodeWithProperties] = {
    val propName = EsUtil.labelName
    filterNodes(NFAnd(NFContainsWith(propName, label), expr))
  }

  override def getNodeById(id: Long): Option[NodeWithProperties] = {
    val propName = EsUtil.idName
    filterNodes(NFEquals(propName, Values.of(id))).headOption
  }

  override def close(ctx: PandaModuleContext): Unit = {
    esClient.close()
  }

  override def start(ctx: PandaModuleContext): Unit = {
  }

  override def beginWriteTransaction(): PropertyWriteTransaction = {
    new BufferedExternalPropertyWriteTransaction(this,
      new InEsGroupedOpVisitor(true, esClient, indexName, typeName),
      new InEsGroupedOpVisitor(false, esClient, indexName, typeName))
  }
}


class InEsGroupedOpVisitor(isCommit: Boolean, esClient: RestHighLevelClient, indexName: String, typeName: String)
  extends GroupedOpVisitor {

  var oldState = mutable.Map[Long, MutableNodeWithProperties]()
  var newState = mutable.Map[Long, MutableNodeWithProperties]()

  def addNodes(docsToAdded: Iterable[NodeWithProperties]): Unit = {
    docsToAdded.map { x =>
      val builder = XContentFactory.jsonBuilder
      builder.startObject
      builder.field(EsUtil.idName, x.id)
      builder.field(EsUtil.labelName, x.labels.toArray[String])
      x.props.foreach(y => {
        builder.field(y._1, EsUtil.neo4jValueToScala(y._2))
      })
      builder.endObject()
      EsUtil.addData(esClient, indexName, typeName, x.id.toString, builder)
    }
  }

  def getNodeWithPropertiesById(nodeId: Long): NodeWithProperties = {
    val dataMap = EsUtil.getData(esClient, indexName, typeName, nodeId.toString)
    EsUtil.sourceMapToNodeWithProperties(dataMap.toMap)
  }

  def deleteNodes(docsToBeDeleted: Iterable[Long]): Unit = {
    docsToBeDeleted.foreach(node => EsUtil.deleteData(esClient, indexName, typeName, node.toString))
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

  def getEsNodeById(id: Long): Map[String, Object] = {
    EsUtil.getData(esClient, indexName, typeName, id.toString).toMap
  }

  override def visitUpdateNode(nodeId: Long, addedProps: Map[String, Value],
                               updateProps: Map[String, Value], removeProps: Array[String],
                               addedLabels: Array[String], removedLabels: Array[String]): Unit = {

    if (isCommit) {
      val doc = getEsNodeById(nodeId)

      val node = EsUtil.sourceMapToNodeWithProperties(doc)
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