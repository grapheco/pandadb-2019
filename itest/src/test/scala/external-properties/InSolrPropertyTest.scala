

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
    transaction.addProperty(1, "database", Values.of("pandaDB"))

    transaction.commit()

    transaction.close()
    Assert.assertEquals(1, solrNodeStore.getRecorderSize)
    transaction = solrNodeStore.beginWriteTransaction()
    val name = transaction.getPropertyValue(1, "database")
    Assert.assertEquals("pandaDB", name.get.asObject())

    transaction.removeProperty(1, "database")
    transaction.commit()
    transaction.close()
    Assert.assertEquals(1, solrNodeStore.getRecorderSize)

    val node = solrNodeStore.getNodeById(1).head.mutable()
    Assert.assertEquals(true, node.props.isEmpty)

    solrNodeStore.clearAll()
    Assert.assertEquals(0, solrNodeStore.getRecorderSize)
  }

  //test for undo
  @Test
  def test3() {
    val solrNodeStore = new InSolrPropertyNodeStore(zkString, collectionName)
    solrNodeStore.clearAll()
    Assert.assertEquals(0, solrNodeStore.getRecorderSize)
    var transaction = solrNodeStore.beginWriteTransaction()
    transaction.addNode(1)
    transaction.addNode(2)
    transaction.addLabel(1, "person")
    transaction.addProperty(2, "name", Values.of("pandaDB"))
    val undo = transaction.commit()
    Assert.assertEquals(2, solrNodeStore.getRecorderSize)
    val node1 = solrNodeStore.getNodeById(1)
    val node2 = solrNodeStore.getNodeById(2)
    Assert.assertEquals("person", node1.head.mutable().labels.head)
    Assert.assertEquals("pandaDB", node2.head.mutable().props.get("name").get.asObject())
    undo.undo()
    transaction.close()
    Assert.assertEquals(0, solrNodeStore.getRecorderSize)
    solrNodeStore.clearAll()
    Assert.assertEquals(0, solrNodeStore.getRecorderSize)
  }
}
