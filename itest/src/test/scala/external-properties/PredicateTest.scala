package Externalproperties

import java.io.{File, FileInputStream}
import java.util.Properties

import cn.pandadb.externalprops.{InMemoryPropertyNodeStore, InSolrPropertyNodeStore, MutableNodeWithProperties, NodeWithProperties}
import org.junit.{Assert, Test}
import org.neo4j.cypher.internal.runtime.interpreted.{NFLessThan, NFPredicate, _}
import org.neo4j.driver.{AuthTokens, GraphDatabase, Transaction, TransactionWork}
import org.neo4j.values.{AnyValue, AnyValues}
import org.neo4j.values.storable.Values

import scala.collection.mutable.ArrayBuffer

/**
 * Created by codeBabyLin on 2019/12/6.
 */

class PredicateTest {

  val configFile = new File("./testdata/neo4j.conf")
  val props = new Properties()
  props.load(new FileInputStream(configFile))
  val zkString = props.getProperty("external.properties.store.solr.zk")
  val collectionName = props.getProperty("external.properties.store.solr.collection")

  def prepareData(solrNodeStore: InSolrPropertyNodeStore): Int = {

    val node1 = MutableNodeWithProperties(1)
    node1.labels += "database"
    node1.props += "name" -> Values.of("pandaDB")
    node1.props += "age" -> Values.of(1)
    node1.props += "nation" -> Values.of("China")

    val node2 = MutableNodeWithProperties(2)
    node2.labels += "database"
    node2.props += "name" -> Values.of("neo4j")
    node2.props += "age" -> Values.of(5)

    val node3 = MutableNodeWithProperties(3)
    node3.labels += "person"
    node3.props += "name" -> Values.of("bluejoe")
    node3.props += "age" -> Values.of(40)

    val node4 = MutableNodeWithProperties(4)
    node4.labels += "person"
    node4.props += "name" -> Values.of("jason")
    node4.props += "age" -> Values.of(39)

    val node5 = MutableNodeWithProperties(5)
    node5.labels += "person"
    node5.props += "name" -> Values.of("Airzihao")
    node5.props += "age" -> Values.of(18)

    val nodeArray = ArrayBuffer[NodeWithProperties]()
    nodeArray += NodeWithProperties(node1.id, node1.props.toMap, node1.labels)
    nodeArray += NodeWithProperties(node2.id, node2.props.toMap, node2.labels)
    nodeArray += NodeWithProperties(node3.id, node3.props.toMap, node3.labels)
    nodeArray += NodeWithProperties(node4.id, node4.props.toMap, node4.labels)
    nodeArray += NodeWithProperties(node5.id, node5.props.toMap, node5.labels)
    solrNodeStore.addNodes(nodeArray)
    solrNodeStore.getRecorderSize
  }

  @Test
  def test3() {
    val solrNodeStore = new InSolrPropertyNodeStore(zkString, collectionName)
    solrNodeStore.clearAll()
    Assert.assertEquals(0, solrNodeStore.getRecorderSize)

    Assert.assertEquals(5, prepareData(solrNodeStore))

    val nodeList1 = solrNodeStore.getNodesByLabel("person")
    val nodeList2 = solrNodeStore.getNodesByLabel("database")

    Assert.assertEquals(3, nodeList1.size)
    Assert.assertEquals(2, nodeList2.size)

    var res1 = solrNodeStore.filterNodesWithProperties(NFGreaterThan("age", Values.of(39)))
    Assert.assertEquals(1, res1.size)

    res1 = solrNodeStore.filterNodesWithProperties(NFGreaterThanOrEqual("age", Values.of(39)))
    Assert.assertEquals(2, res1.size)

    res1 = solrNodeStore.filterNodesWithProperties(NFLessThan("age", Values.of(18)))
    Assert.assertEquals(2, res1.size)

    res1 = solrNodeStore.filterNodesWithProperties(NFLessThanOrEqual("age", Values.of(18)))
    Assert.assertEquals(3, res1.size)

    res1 = solrNodeStore.filterNodesWithProperties(NFEquals("age", Values.of(18)))
    Assert.assertEquals("Airzihao", res1.head.mutable().props.get("name").get.asObject())

    res1 = solrNodeStore.filterNodesWithProperties(NFContainsWith("name", "joe"))
    Assert.assertEquals(1, res1.size)
    Assert.assertEquals("bluejoe", res1.head.mutable().props.get("name").get.asObject())

    res1 = solrNodeStore.filterNodesWithProperties(NFEndsWith("name", "son"))
    Assert.assertEquals(1, res1.size)
    Assert.assertEquals(39.toLong, res1.head.mutable().props.get("age").get.asObject())

    res1 = solrNodeStore.filterNodesWithProperties(NFStartsWith("name", "pan"))
    Assert.assertEquals(1, res1.size)
    Assert.assertEquals("database", res1.head.labels.head)

    res1 = solrNodeStore.filterNodesWithProperties(NFStartsWith("name", "pan"))
    Assert.assertEquals(1, res1.size)
    Assert.assertEquals("database", res1.head.labels.head)

    res1 = solrNodeStore.filterNodesWithProperties(NFFalse())
    Assert.assertEquals(0, res1.size)

    res1 = solrNodeStore.filterNodesWithProperties(NFTrue())
    Assert.assertEquals(5, res1.size)

    res1 = solrNodeStore.filterNodesWithProperties(NFHasProperty("nation"))
    Assert.assertEquals(1, res1.size)
    Assert.assertEquals("pandaDB", res1.head.props.get("name").get.asObject())

    res1 = solrNodeStore.filterNodesWithProperties(NFIsNull("nation"))
    Assert.assertEquals(4, res1.size)

    res1 = solrNodeStore.filterNodesWithProperties(NFNotNull("nation"))
    Assert.assertEquals(1, res1.size)
    Assert.assertEquals("China", res1.head.props.get("nation").get.asObject())

    res1 = solrNodeStore.filterNodesWithProperties(NFNotEquals("age", Values.of(18)))
    Assert.assertEquals(4, res1.size)

    res1 = solrNodeStore.filterNodesWithProperties(NFRegexp("name", ".?lue.*"))
    Assert.assertEquals(40, res1.head.mutable().props.get("age").get.asObject().toString.toLong)

    res1 = solrNodeStore.filterNodesWithProperties(NFAnd(NFIsNull("nation"), NFLessThanOrEqual("age", Values.of(18))))
    Assert.assertEquals(2, res1.size)

    res1 = solrNodeStore.filterNodesWithProperties(NFNot(NFIsNull("nation")))
    Assert.assertEquals(1, res1.size)
    Assert.assertEquals("China", res1.head.props.get("nation").get.asObject())

    res1 = solrNodeStore.filterNodesWithProperties(NFOr(NFNotNull("nation"), NFGreaterThanOrEqual("age", Values.of(40))))
    Assert.assertEquals(2, res1.size)

    solrNodeStore.clearAll()
    Assert.assertEquals(0, solrNodeStore.getRecorderSize)
  }

}
