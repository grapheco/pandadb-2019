
import java.io.File

import scala.collection.JavaConverters._
import cn.pandadb.server.PNodeServer
import org.junit.{After, Before, Test}
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.graphdb.{GraphDatabaseService, Label}
import org.neo4j.io.fs.FileUtils
import cn.pandadb.externalprops.{InMemoryPropertyNodeStore, InMemoryPropertyNodeStoreFactory}


class UpdatePropertyQueryAPITest extends UpdateQueryTestBase {
  val tmpns = InMemoryPropertyNodeStore

  @Test
  def test1(): Unit = {
    // update and add node properties

    // create node
    val tx = db.beginTx()
    val label1 = Label.label("Person")
    val node1 = db.createNode(label1)
    // before tx close, data haven't flush to store
    assert(tmpns.nodes.size == 0)
    tx.success()
    tx.close()
    // after tx close, data flushed to store
    val id1 = node1.getId
    assert(id1 != -1 )
    assert(tmpns.nodes.size == 1)
    assert(tmpns.nodes.get(id1).get.props.size == 0)
    assert(tmpns.nodes.get(id1).get.labels.size == 1 && tmpns.nodes.get(id1).get.labels.toList(0) == "Person")

    // add properties

    val tx2 = db.beginTx()
    node1.setProperty("name", "test01")
    node1.setProperty("age", 10)
    node1.setProperty("sex", "male")
    // before tx close, data haven't flush to store
    assert(tmpns.nodes.size == 1)
    assert(tmpns.nodes.get(id1).get.props.size == 0)
    tx2.success()
    tx2.close()
    // after tx close, data flushed to store
    assert(tmpns.nodes.size == 1)
    val fields = tmpns.nodes.get(id1).get.props
    assert(fields.size == 3 && fields("name").equals("test01") && fields("age").equals(10) &&
                     fields("sex").equals("male"))

    // update properties
    val tx3 = db.beginTx()
    node1.setProperty("name", "test02")
    node1.setProperty("age", 20)
    // before tx close, data haven't flush to store
    val fields2 = tmpns.nodes.get(id1).get.props
    assert(fields2.size == 3 && fields2("name").equals("test01") && fields2("age").equals(10) &&
      fields2("sex").equals("male"))
    tx3.success()
    tx3.close()
    // after tx close, data flushed to store
    assert(tmpns.nodes.size == 1)
    val fields3 = tmpns.nodes.get(id1).get.props
    assert(fields3.size == 3 && fields3("name").equals("test02") && fields3("age").equals(20) &&
            fields3("sex").equals("male"))
  }

  @Test
  def test2(): Unit = {
    // delete node properties

    // create node
    val tx1 = db.beginTx()
    val label1 = Label.label("Person")
    val node1 = db.createNode(label1)
    node1.setProperty("name", "test01")
    node1.setProperty("age", 10)
    node1.setProperty("sex", "male")
    val id1 = node1.getId
    tx1.success()
    tx1.close()
    // after tx close, data flushed to store
    assert(tmpns.nodes.size == 1)
    val fields = tmpns.nodes.get(id1).get.props
    assert(fields.size == 3 && fields("name").equals("test01") && fields("age").equals(10) &&
      fields("sex").equals("male"))

    // delete properties
    val tx2 = db.beginTx()
    node1.removeProperty("name")
    node1.removeProperty("age")
    // before tx close, data haven't flush to store
    assert(tmpns.nodes.size == 1)
    val fields1 = tmpns.nodes.get(id1).get.props
    assert(fields1.size == 3 && fields1("name").equals("test01") && fields1("age").equals(10) &&
      fields1("sex").equals("male"))
    tx2.success()
    tx2.close()
    // after tx close, data flushed to store
    assert(tmpns.nodes.size == 1)
    val fields2 = tmpns.nodes.get(id1).get.props
    assert(fields2.size == 1 && fields2("sex").equals("male"))

  }

}


class UpdateLabelQueryAPITest extends UpdateQueryTestBase {
  val tmpns = InMemoryPropertyNodeStore


  @Test
  def test1(): Unit = {
    // add labels

    // create node
    val tx = db.beginTx()
    val label1 = Label.label("Person")
    val node1 = db.createNode(label1)
    // before tx close, data haven't flush to store
    assert(tmpns.nodes.size == 0)
    tx.success()
    tx.close()
    // after tx close, data flushed to store
    val id1 = node1.getId
    assert(tmpns.nodes.size == 1)
    assert(tmpns.nodes.get(id1).get.labels.size == 1 && tmpns.nodes.get(id1).get.labels.toList(0) == "Person")

    // add label
    val tx1 = db.beginTx()
    val label2 = Label.label("Man")
    node1.addLabel(label2)
    // before tx close, data haven't flush to store
    assert(tmpns.nodes.size == 1)
    assert(tmpns.nodes.get(id1).get.labels.size == 1 )
    tx1.success()
    tx1.close()

    // after tx close, data flushed to store
    assert(tmpns.nodes.size == 1)
    val labels = tmpns.nodes.get(id1).get.labels.toList
    assert(labels.size == 2 && labels.contains("Person") && labels.contains("Man") )
  }

  @Test
  def test2(): Unit = {
    // remove one label
    // create node
    val tx = db.beginTx()
    val label1 = Label.label("Person")
    val label2 = Label.label("Boy")
    val label3 = Label.label("Man")
    val node1 = db.createNode(label1, label2, label3)
    // before tx close, data haven't flush to store
    assert(tmpns.nodes.size == 0)
    tx.success()
    tx.close()
    // after tx close, data flushed to store
    val id1 = node1.getId
    assert(tmpns.nodes.size == 1)
    val labels = tmpns.nodes.get(id1).get.labels.toList
    assert(labels.size == 3 && labels.contains("Person") && labels.contains("Man") && labels.contains("Boy"))

    // add label
    val tx1 = db.beginTx()
    node1.removeLabel(label2)
    // before tx close, data haven't flush to store
    assert(tmpns.nodes.size == 1)
    assert(tmpns.nodes.get(id1).get.labels.size == 3 )
    val labels2 = tmpns.nodes.get(id1).get.labels.toList
    assert(labels2.size == 3 && labels2.contains("Person") && labels2.contains("Man") && labels2.contains("Boy"))
    tx1.success()
    tx1.close()

    // after tx close, data flushed to store
    assert(tmpns.nodes.size == 1)
    val labels3 = tmpns.nodes.get(id1).get.labels.toList
    assert(labels3.size == 2 && labels3.contains("Person") && labels3.contains("Man") )
  }

}