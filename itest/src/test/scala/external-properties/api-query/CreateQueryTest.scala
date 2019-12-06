
import java.io.File
import java.time.ZoneId
import scala.collection.JavaConverters._
import cn.pandadb.server.PNodeServer
import org.junit.{After, Assert, Before, Test}
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.graphdb.{GraphDatabaseService, Result, Label}
import org.neo4j.io.fs.FileUtils
import cn.pandadb.externalprops.{InMemoryPropertyNodeStore, InMemoryPropertyNodeStoreFactory}
import org.neo4j.values.storable.{DateTimeValue, DateValue, LocalDateTimeValue, TimeValue}


class CreateNodeQueryAPITest extends CreateQueryTestBase {
  val tmpns = InMemoryPropertyNodeStore

  @Test
  def test1(): Unit = {
    // create one node
    val tx1 = db.beginTx()
    val label1 = Label.label("Person")
    val node1 = db.createNode(label1)

    // before tx close, data haven't flush to store
    assert(tmpns.nodes.size == 0)
    tx1.success()
    tx1.close()
    // after tx close, data flushed to store
    val id1 = node1.getId
    assert(id1 != -1 )
    assert(tmpns.nodes.size == 1)
    assert(tmpns.nodes.get(id1).get.props.size == 0)
    assert(tmpns.nodes.get(id1).get.labels.size == 1 && tmpns.nodes.get(id1).get.labels.toList(0) == "Person")

    val tx2 = db.beginTx()
    val node2 = db.createNode()
    // before tx close, data haven't flush to store
    assert(tmpns.nodes.size == 1)
    tx2.success()
    tx2.close()

    // after tx close, data flushed to store
    val id2 = node2.getId
    assert(id2 != -1)
    assert(tmpns.nodes.size == 2)
    assert(tmpns.nodes.get(id2).get.props.size == 0)
    assert(tmpns.nodes.get(id2).get.labels == null || tmpns.nodes.get(id2).get.labels.size == 0)
  }

  @Test
  def test2(): Unit = {
    // create node with labels and properties
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

    // before tx close, data haven't flush to store
    assert(tmpns.nodes.size == 0)
    tx.success();
    tx.close()

    var id1: Long = node1.getId
    var id2: Long = node2.getId

    // after tx close, data flushed to store
    assert(tmpns.nodes.get(id1).size == 1 && tmpns.nodes.get(id2).size == 1)

    val fields1 = tmpns.nodes.get(id1).get.props
    assert(fields1.size == 3 && fields1("name").equals("test01") && fields1("age").equals(10) && fields1("adult").equals(false) )
    val labels1 = tmpns.nodes.get(id1).get.labels.toList
    assert(labels1.size == 1 && labels1(0) == "Person")

    val fields2 = tmpns.nodes.get(id2).get.props
    assert(fields2.size == 3 && fields2("name").equals("test02")  && fields2("age").equals(20) && fields2("adult").equals(true)  )
    val labels2 = tmpns.nodes.get(id2).get.labels.toList
    assert(labels2.size == 2 && labels2.contains("Person") && labels2.contains("Man") )

  }

  @Test
  def test3(): Unit = {
    // create node with DateTime type property and array value
    val tx = db.beginTx()
    val label1 = Label.label("Person")
    val node1 = db.createNode(label1)
    node1.setProperty("name", "test01")
    node1.setProperty("age", 10)
    node1.setProperty("adult", false)

    val born1 = DateValue.date(2019, 1, 1)
    val born2 = TimeValue.time(12, 5, 1, 0, "Z")
    val born3 = DateTimeValue.datetime(2019, 1, 2,
      12, 5, 15, 0, "Australia/Eucla")
    val born4 = DateTimeValue.datetime(2015, 6, 24,
      12, 50, 35, 556, ZoneId.of("Z"))
    node1.setProperty("born1", born1)
    node1.setProperty("born2", born2)
    node1.setProperty("born3", born3)
    node1.setProperty("born4", born4)

    val arr1 = Array(1, 2, 3)
    val arr2 = Array("aa", "bb", "cc")
    val arr3 = Array(true, false)
    node1.setProperty("arr1", arr1)
    node1.setProperty("arr2", arr2)
    node1.setProperty("arr3", arr3)

    assert(tmpns.nodes.size == 0)
    tx.success();
    tx.close()

    var id1: Long = node1.getId
    assert(tmpns.nodes.size == 1)
    assert(tmpns.nodes.get(id1).size == 1 )

    val fields1 = tmpns.nodes.get(id1).get.props
    assert(fields1.size == 10 )
    assert(fields1("born1").asInstanceOf[DateValue].equals(born1))
    assert(fields1("born2").asInstanceOf[TimeValue].equals(born2))
    assert(fields1("born3").equals(born3))
    assert(fields1("born4").equals(born4))
    assert(fields1("arr1").equals(arr1))
    assert(fields1("arr2").equals(arr2))
    assert(fields1("arr3").equals(arr3))

  }


}
