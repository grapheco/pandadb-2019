import java.io.{File, FileInputStream}
import java.util.Properties

import cn.pandadb.externalprops.{InSolrPropertyNodeStore, MutableNodeWithProperties, NodeWithProperties, SolrQueryResults}
import org.apache.solr.client.solrj.SolrQuery
import org.junit.{Assert, Test}
import org.neo4j.values.storable.Values

import scala.collection.mutable.ArrayBuffer

class SolrIterableTest {

    val configFile = new File("./testdata/neo4j.conf")
    val props = new Properties()
    props.load(new FileInputStream(configFile))
    val zkString = props.getProperty("external.properties.store.solr.zk")
    val collectionName = props.getProperty("external.properties.store.solr.collection")

    //val collectionName = "test"

    //test for node label add and remove
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
      nodeArray += NodeWithProperties(node1.id + 5, node1.props.toMap, node1.labels)
      nodeArray += NodeWithProperties(node2.id + 5, node2.props.toMap, node2.labels)
      nodeArray += NodeWithProperties(node3.id + 5, node3.props.toMap, node3.labels)
      nodeArray += NodeWithProperties(node4.id + 5, node4.props.toMap, node4.labels)
      nodeArray += NodeWithProperties(node5.id + 5, node5.props.toMap, node5.labels)
      nodeArray += NodeWithProperties(node1.id + 10, node1.props.toMap, node1.labels)
      nodeArray += NodeWithProperties(node2.id + 10, node2.props.toMap, node2.labels)
      nodeArray += NodeWithProperties(node3.id + 10, node3.props.toMap, node3.labels)
      nodeArray += NodeWithProperties(node4.id + 10, node4.props.toMap, node4.labels)
      nodeArray += NodeWithProperties(node5.id + 10, node5.props.toMap, node5.labels)
      solrNodeStore.addNodes(nodeArray)
      solrNodeStore.getRecorderSize
    }
     //scalastyle:off
    @Test
    def test1() {
      val solrNodeStore = new InSolrPropertyNodeStore(zkString, collectionName)
      solrNodeStore.clearAll()
      Assert.assertEquals(0, solrNodeStore.getRecorderSize)
      Assert.assertEquals(15, prepareData(solrNodeStore))
      val query = new SolrQuery("*:*")
      val res = new SolrQueryResults(solrNodeStore._solrClient, query, 5)
      val it = res.iterator()
      it.getCurrentData().foreach(u => println(u))
      while (it.readNextPage()) {
        it.getCurrentData().foreach(u => println(u))
      }
     // res.getAllResults().foreach(u => println(u))

    }

}
