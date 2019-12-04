
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
    tx1.success()
    tx1.close()
    val id1 = node1.getId
    assert(id1 != -1 )
    assert(tmpns.nodes.size == 1)
    assert(tmpns.nodes.get(id1).get.props.size == 0)
    assert(tmpns.nodes.get(id1).get.labels.size == 1 && tmpns.nodes.get(id1).get.labels.toList(0) == "Person")

    val tx2 = db.beginTx()
    val node2 = db.createNode()
    tx2.success()
    tx2.close()

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
    val node2 = db.createNode(label1, label2)
    val query =
      """CREATE (n1:Person { name:'test01', age:10, adult:False})
        |CREATE (n2:Person:Man { name:'test02', age:20, adult:True})
        |RETURN id(n1),id(n2)
      """.stripMargin
    val rs = db.execute(query)
    var id1: Long = -1
    var id2: Long = -1
    if (rs.hasNext) {
      val row = rs.next()
      id1 = row.get("id(n1)").toString.toLong
      id2 = row.get("id(n2)").toString.toLong
    }

    assert(tmpns.nodes.get(id1).size == 1 && tmpns.nodes.get(id2).size == 1)

    val fields1 = tmpns.nodes.get(id1).get.props
    assert(fields1.size == 3 && fields1("name").equals("test01") && fields1("age").equals(10) && fields1("adult").equals(false) )
    val labels1 = tmpns.nodes.get(id1).get.labels.toList
    assert(labels1.size == 1 && labels1(0) == "Person")

    val fields2 = tmpns.nodes.get(id2).get.props
    assert(fields2.size == 3 && fields2("name").equals("test02")  && fields2("age").equals(20) && fields2("adult").equals(true)  )
    val labels2 = tmpns.nodes.get(id2).get.labels.toList
    assert(labels2.size == 2 && labels2.contains("Person") && labels2.contains("Man") )

    tx.success();
    tx.close()
  }

}
