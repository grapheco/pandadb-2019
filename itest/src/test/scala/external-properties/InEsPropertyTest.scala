

package Externalproperties

import java.io.{File, FileInputStream}
import java.util.Properties

import scala.collection.JavaConversions._
import org.apache.http.HttpHost
import org.junit.{Assert, Test}
import org.elasticsearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.elasticsearch.action.admin.indices.create.{CreateIndexRequest, CreateIndexResponse}
import org.elasticsearch.action.index.{IndexRequest, IndexResponse}
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.action.delete.{DeleteRequest, DeleteResponse}
import org.elasticsearch.action.get.{GetRequest, GetResponse}
import org.elasticsearch.action.update.{UpdateRequest, UpdateResponse}
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.common.Strings
import org.elasticsearch.index.query.{QueryBuilder, QueryBuilders}
import org.elasticsearch.search.fetch.subphase.FetchSourceContext
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.action.search.{SearchRequest, SearchResponse}
import org.elasticsearch.index.reindex.{BulkByScrollResponse, DeleteByQueryRequest}
import org.elasticsearch.script.ScriptType
import org.elasticsearch.script.mustache.SearchTemplateRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.script.mustache.SearchTemplateResponse
import cn.pandadb.externalprops.{InElasticSearchPropertyNodeStore, NodeWithProperties}
import org.neo4j.values.storable.Values
import com.alibaba.fastjson.JSONObject

/**
 * Created by codeBabyLin on 2019/12/5.
 */

class InEsPropertyTest {
  val host = "10.0.82.216"
  val port = 9200
  val indexName = "test0113"
  val typeName = "doc"

  val httpHost = new HttpHost(host, port, "http")
  val builder = RestClient.builder(httpHost)
  val client = new RestHighLevelClient(builder)

  @Test
  def test1(): Unit = {
    val esNodeStore = new InElasticSearchPropertyNodeStore(host, port, indexName, typeName)
    esNodeStore.clearAll()
    Assert.assertEquals(0, esNodeStore.getRecorderSize)
    var transaction = esNodeStore.beginWriteTransaction()
    transaction.addNode(1)
    transaction.addLabel(1, "person")
    transaction.addLabel(1, "people")
    transaction.commit()
    transaction.close()
    Assert.assertEquals(1, esNodeStore.getRecorderSize)
    transaction = esNodeStore.beginWriteTransaction()
    var label = transaction.getNodeLabels(1)
    Assert.assertEquals(2, label.size)
    Assert.assertEquals("person", label.head)
    Assert.assertEquals("people", label.last)
    transaction.removeLabel(1, "people")
    label = transaction.getNodeLabels(1)
    Assert.assertEquals(1, label.size)
    Assert.assertEquals("person", label.head)
    transaction.commit()
    transaction.close()
    esNodeStore.clearAll()
    Assert.assertEquals(0, esNodeStore.getRecorderSize)
  }

  // test for node property add and remove
  @Test
  def test2() {
    val esNodeStore = new InElasticSearchPropertyNodeStore(host, port, indexName, typeName)
    esNodeStore.clearAll()
    Assert.assertEquals(0, esNodeStore.getRecorderSize)
    var transaction = esNodeStore.beginWriteTransaction()
    transaction.addNode(1)
    transaction.addProperty(1, "database", Values.of("pandaDB"))

    transaction.commit()

    transaction.close()
    Assert.assertEquals(1, esNodeStore.getRecorderSize)
    transaction = esNodeStore.beginWriteTransaction()
    val name = transaction.getPropertyValue(1, "database")
    Assert.assertEquals("pandaDB", name.get.asObject())

    transaction.removeProperty(1, "database")
    transaction.commit()
    transaction.close()
    Assert.assertEquals(1, esNodeStore.getRecorderSize)

    val node = esNodeStore.getNodeById(1).head.mutable()
    Assert.assertEquals(true, node.props.isEmpty)

    esNodeStore.clearAll()
    Assert.assertEquals(0, esNodeStore.getRecorderSize)
  }

  //test for undo
  @Test
  def test3() {
    val esNodeStore = new InElasticSearchPropertyNodeStore(host, port, indexName, typeName)
    esNodeStore.clearAll()
    Assert.assertEquals(0, esNodeStore.getRecorderSize)
    var transaction = esNodeStore.beginWriteTransaction()
    transaction.addNode(1)
    transaction.addNode(2)
    transaction.addLabel(1, "person")
    transaction.addProperty(2, "name", Values.of("pandaDB"))
    val undo = transaction.commit()
    Assert.assertEquals(2, esNodeStore.getRecorderSize)
    val node1 = esNodeStore.getNodeById(1)
    val node2 = esNodeStore.getNodeById(2)
    Assert.assertEquals("person", node1.head.mutable().labels.head)
    Assert.assertEquals("pandaDB", node2.head.mutable().props.get("name").get.asObject())
    undo.undo()
    transaction.close()
    Assert.assertEquals(0, esNodeStore.getRecorderSize)
  }

  // test for node property value
  @Test
  def test4() {
    val esNodeStore = new InElasticSearchPropertyNodeStore(host, port, indexName, typeName)
    esNodeStore.clearAll()
    Assert.assertEquals(0, esNodeStore.getRecorderSize)
    var transaction = esNodeStore.beginWriteTransaction()
    transaction.addNode(1)
    transaction.addLabel(1, "Person")
    transaction.addLabel(1, "Man")
    transaction.addProperty(1, "name", Values.of("test"))
    transaction.addProperty(1, "arr1", Values.of(100))
    transaction.addProperty(1, "arr2", Values.of(155.33))
    transaction.addProperty(1, "arr3", Values.of(true))
    transaction.addProperty(1, "arr4", Values.of(Array(1, 2, 3)))
    Assert.assertEquals(0, esNodeStore.getRecorderSize)
    transaction.commit()
    transaction.close()
    Assert.assertEquals(1, esNodeStore.getRecorderSize)
    val node: NodeWithProperties = esNodeStore.getNodeById(1).get
    Assert.assertEquals(1, node.id)
    val nodeLabels = node.labels.toArray
    Assert.assertEquals(2, nodeLabels.size)
    Assert.assertEquals(true, nodeLabels.contains("Man") && nodeLabels.contains("Person"))
    assert(node.props.get("name").get.equals("test"))
    assert(node.props.get("arr1").get.equals(100))
    assert(node.props.get("arr2").get.equals(155.33))
    assert(node.props.get("arr3").get.equals(true))
    assert(node.props.get("arr4").get.equals(Array(1, 2, 3)))
  }
}
