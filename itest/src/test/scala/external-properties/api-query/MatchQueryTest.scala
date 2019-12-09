
import java.io.File

import org.junit.{Assert, Test}
import org.neo4j.graphdb.{GraphDatabaseService, Label, Result}

import scala.collection.JavaConverters._
import cn.pandadb.server.{GlobalContext, PNodeServer}
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.io.fs.FileUtils
import cn.pandadb.externalprops.{CustomPropertyNodeStore, InMemoryPropertyNodeStore, InMemoryPropertyNodeStoreFactory}
import scala.collection.mutable

class MatchQueryAPITest extends MatchQueryTestBase {

  @Test
  def test1(): Unit = {
    // initData
    val tx = db.beginTx()
    val label1 = Label.label("Person")
    val label2 = Label.label("Man")
    val node1 = db.createNode(label1)
    node1.setProperty("name", "test01")
    node1.setProperty("age", 10)
    node1.setProperty("adult", false)
    val node2 = db.createNode(label1, label2)
    node2.setProperty("name", "test02")
    node2.setProperty("age", 20)
    node2.setProperty("adult", true)
    tx.success()
    tx.close()

    // test getAllNodes()
    val tx1 = db.beginTx()
    val nodes1 = db.getAllNodes().iterator()
    var count = 0
    while (nodes1.hasNext) {
      count += 1
      nodes1.next()
    }
    tx1.close()
    assert(2 == count)

    // test getLabels()
    val tx2 = db.beginTx()
    val n1 = db.getNodeById(node1.getId)
    val labels1 = n1.getLabels().asScala
    for (label: Label <- labels1) {
      assert(label.name() == "Person")
    }
    tx2.close()

    // test getAllProperties()
    val tx3 = db.beginTx()
    val n2 = db.getNodeById(node1.getId)
    val props2 = n2.getAllProperties()
    assert(props2.get("name").equals("test01"))
    assert(props2.get("age").equals(10))
    assert(props2.get("adult").equals(false))
    tx3.close()

    // test getProperty()
    val tx4 = db.beginTx()
    val n4 = db.getNodeById(node2.getId)
    assert(n4.getProperty("name").equals("test02"))
    assert(n4.getProperty("age").equals(20))
    assert(n4.getProperty("adult").equals(true))
    tx4.close()
  }

}
