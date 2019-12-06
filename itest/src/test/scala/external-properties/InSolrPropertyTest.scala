

package Externalproperties

import java.io.{File, FileInputStream}
import java.util.Properties

import cn.pandadb.externalprops.{InMemoryPropertyNodeStore, InSolrPropertyNodeStore, MutableNodeWithProperties, NodeWithProperties}
import org.junit.{Assert, Test}
import org.neo4j.driver.{AuthTokens, GraphDatabase, Transaction, TransactionWork}
import org.neo4j.values.{AnyValue, AnyValues}
import org.neo4j.values.storable.Values

/**
 * Created by codeBabyLin on 2019/12/5.
 */

class InSolrPropertyTest {

  val configFile = new File("./testdata/neo4j.conf")
  val props = new Properties()
  props.load(new FileInputStream(configFile))
  val zkString = props.getProperty("external.properties.store.solr.zk")
  val collectionName = props.getProperty("external.properties.store.solr.collection")

  //val collectionName = "test"



//test for node label add and remove
  @Test
  def test1() {
    val solrNodeStore = new InSolrPropertyNodeStore(zkString, collectionName)
    solrNodeStore.clearAll()
    Assert.assertEquals(0, solrNodeStore.getRecorderSize)
    var transaction = solrNodeStore.beginWriteTransaction()
    transaction.addNode(1)
    transaction.addLabel(1, "person")
    transaction.addLabel(1, "people")
    transaction.commit()
    transaction.close()
    Assert.assertEquals(1, solrNodeStore.getRecorderSize)
    transaction = solrNodeStore.beginWriteTransaction()
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
    solrNodeStore.clearAll()
    Assert.assertEquals(0, solrNodeStore.getRecorderSize)
  }

// test for node property add and remove
  @Test
  def test2() {
    val solrNodeStore = new InSolrPropertyNodeStore(zkString, collectionName)
    solrNodeStore.clearAll()
    Assert.assertEquals(0, solrNodeStore.getRecorderSize)
    var transaction = solrNodeStore.beginWriteTransaction()
    transaction.addNode(1)
    transaction.addProperty(1, "namehj", Values.of("pandaDB"))

    transaction.commit()

    transaction.close()

    //transaction = solrNodeStore.beginWriteTransaction()

    transaction = solrNodeStore.beginWriteTransaction()
    val name = transaction.getPropertyValue(1, "namehj")
    Assert.assertEquals("pandaDB", Values.values(name.get.asObject()).toString)
   // Assert.assertEquals("person", label.head)
   // Assert.assertEquals("people", label.last)
   // Assert.assertEquals(1, solrNodeStore.getRecorderSize)
   // solrNodeStore.clearAll()
   // Assert.assertEquals(0, solrNodeStore.getRecorderSize)
  }

  @Test
  def test3() {
    val solrNodeStore = new InSolrPropertyNodeStore(zkString, collectionName)
    solrNodeStore.clearAll()
    val node = new MutableNodeWithProperties(1)
    node.labels += "person"
    node.props += "name" -> Values.of("pandaDB")
    node.props += "age" -> Values.of("25")
    solrNodeStore.addNodes(Iterable(NodeWithProperties(node.id, node.props.toMap, node.labels)))
    val node1 = solrNodeStore.getNodeById(1)
    //scalastyle:off println
    //println(node1.get.props.get("age").head.asObject())
    // Assert.assertEquals("person", label.head)
    // Assert.assertEquals("people", label.last)
    // Assert.assertEquals(1, solrNodeStore.getRecorderSize)
    // solrNodeStore.clearAll()
    // Assert.assertEquals(0, solrNodeStore.getRecorderSize)
  }


}
