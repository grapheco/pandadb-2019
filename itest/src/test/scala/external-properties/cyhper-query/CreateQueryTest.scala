
import java.io.File
import java.time.ZoneId

import scala.collection.JavaConverters._
import cn.pandadb.server.PNodeServer
import org.junit.{After, Assert, Before, Test}
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.graphdb.{GraphDatabaseService, Result}
import org.neo4j.io.fs.FileUtils
import cn.pandadb.externalprops.{CustomPropertyNodeStore, InMemoryPropertyNodeStore, InMemoryPropertyNodeStoreFactory}
import org.neo4j.values.storable.{DateTimeValue, DateValue, LocalDateTimeValue, TimeValue}
import cn.pandadb.server.GlobalContext

trait CreateQueryTestBase extends QueryTestBase {

}

class CreateNodeQueryTest extends CreateQueryTestBase {
  val tmpns = InMemoryPropertyNodeStore

  @Test
  def test1(): Unit = {
    // create one node
    val query = "create (n1:Person) return id(n1)"
    val rs = db.execute(query)
    var id1: Long = -1
    if (rs.hasNext) {
      val row = rs.next()
      id1 = row.get("id(n1)").toString.toLong
    }
    assert(id1 != -1 )
    assert(tmpns.nodes.size == 1)
    assert(tmpns.nodes.get(id1).get.props.size == 0)
    assert(tmpns.nodes.get(id1).get.labels.size == 1 && tmpns.nodes.get(id1).get.labels.toList(0) == "Person")


    val query2 = "create (n1) return id(n1)"
    val rs2 = db.execute(query2)
    var id2: Long = -1
    if (rs2.hasNext) {
      val row = rs2.next()
      id2 = row.get("id(n1)").toString.toLong
    }
    assert(id2 != -1)
    assert(tmpns.nodes.size == 2)
    assert(tmpns.nodes.get(id2).get.props.size == 0)
    assert(tmpns.nodes.get(id2).get.labels.size == 0)
  }

  @Test
  def test2(): Unit = {
    // create multiple nodes
    val tx = db.beginTx()
    val query = "create (n1:Person),(n2:Man) return id(n1),id(n2)"
    val rs = db.execute(query)
    var id1: Long = 0
    var id2: Long = 0
    if (rs.hasNext) {
      val row = rs.next()
      id1 = row.get("id(n1)").toString.toLong
      id2 = row.get("id(n2)").toString.toLong
    }
    assert(id1 != -1 && id2 != -1)

    // before tx close, data haven't flush to store
    assert(tmpns.nodes.size == 0)
    tx.success()
    tx.close()

    // after tx close, data flushed to store
    assert(tmpns.nodes.size == 2)
    assert(tmpns.nodes.get(id1).size == 1 && tmpns.nodes.get(id2).size == 1)
    assert(tmpns.nodes.get(id1).get.props.size == 0 )
    assert(tmpns.nodes.get(id1).get.labels.size == 1 && tmpns.nodes.get(id1).get.labels.toList(0) == "Person")
    assert(tmpns.nodes.get(id2).get.props.size == 0 )
    assert(tmpns.nodes.get(id2).get.labels.size == 1 && tmpns.nodes.get(id2).get.labels.toList(0) == "Man")
    assert(tmpns.nodes.get(id2).get.labels.size == 1 && tmpns.nodes.get(id2).get.labels.toList(0) == "Man")
  }


  @Test
  def test3(): Unit = {
    // create node with labels and properties
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

    // Results have been visited, tx closed and data haven flush to store
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
  def test4(): Unit = {
    // create node with relationship
    val tx = db.beginTx()
    val query =
      """CREATE (n1:Person { name:'test01', age:10})-[:WorksAt]->(neo:Company{business:'Software'})
        |<-[:Create{from:1987}]-(n2:Ceo { name:'test02', age:20})
        |RETURN id(n1),id(n2), id(neo)
      """.stripMargin
    val rs = db.execute(query)
    var id1: Long = -1
    var id2: Long = -1
    var idNeo: Long = -1
    if (rs.hasNext) {
      val row = rs.next()
      id1 = row.get("id(n1)").toString.toLong
      id2 = row.get("id(n2)").toString.toLong
      idNeo = row.get("id(neo)").toString.toLong
    }

    // before tx close, data haven't flush to store
    assert(tmpns.nodes.size == 0)
    tx.success();
    tx.close()

    // after tx close, data flushed to store
    assert(tmpns.nodes.size == 3)
    assert(tmpns.nodes.get(id1).size == 1 && tmpns.nodes.get(id2).size == 1 && tmpns.nodes.get(idNeo).size == 1)

    val fields1 = tmpns.nodes.get(id1).get.props
    assert(fields1.size == 2 && fields1("name").equals("test01") && fields1("age").equals(10) )
    val labels1 = tmpns.nodes.get(id1).get.labels.toList
    assert(labels1.size == 1 && labels1(0) == "Person")

    val fields2 = tmpns.nodes.get(id2).get.props
    assert(fields2.size == 2 && fields2("name").equals("test02")  && fields2("age").equals(20) )
    val labels2 = tmpns.nodes.get(id2).get.labels.toList
    assert(labels2.size == 1 && labels2.contains("Ceo") )

    val fields3 = tmpns.nodes.get(idNeo).get.props
    assert(fields3.size == 1 && fields3("business").equals("Software"))
    val labels3 = tmpns.nodes.get(idNeo).get.labels.toList
    assert(labels3.size == 1 && labels3.contains("Company"))

  }

  @Test
  def test5(): Unit = {
    // create node with DateTime type property value
    val tx = db.beginTx()
    val query =
      """CREATE (n1:Person { name:'test01',born1:date('2019-01-01'), born2:time('12:05:01')
        |,born3:datetime('2019-01-02T12:05:15[Australia/Eucla]'), born4:datetime('2015-06-24T12:50:35.000000556Z')})
        |RETURN id(n1)
      """.stripMargin
    val rs = db.execute(query)
    var id1: Long = -1
    if (rs.hasNext) {
      val row = rs.next()
      id1 = row.get("id(n1)").toString.toLong
    }
    assert(tmpns.nodes.size == 0)
    tx.success();
    tx.close()

    assert(tmpns.nodes.size == 1)
    assert(tmpns.nodes.get(id1).size == 1 )

    val fields1 = tmpns.nodes.get(id1).get.props
    assert(fields1.size == 5 )
    val born1 = DateValue.date(2019, 1, 1)
    val born2 = TimeValue.time(12, 5, 1, 0, "Z")
    val born3 = DateTimeValue.datetime(2019, 1, 2,
      12, 5, 15, 0, "Australia/Eucla")
    val born4 = DateTimeValue.datetime(2015, 6, 24,
      12, 50, 35, 556, ZoneId.of("Z"))

    assert(fields1("born1").asInstanceOf[DateValue].equals(born1))
    assert(fields1("born2").asInstanceOf[TimeValue].equals(born2))
    assert(fields1("born3").equals(born3))
    assert(fields1("born4").equals(born4))
  }

  @Test
  def test6(): Unit = {
    // create node with Array type property value
    val query =
      """CREATE (n1:Person { name:'test01',titles:["ceo","ui","dev"],
        |salaries:[10000,20000,30597,500954], boolattr:[False,True,false,true]})
        |RETURN id(n1)
      """.stripMargin
    val rs = db.execute(query)
    var id1: Long = -1
    if (rs.hasNext) {
      val row = rs.next ()
      id1 = row.get("id(n1)").toString.toLong
    }

    assert(tmpns.nodes.size == 1)
    assert(tmpns.nodes.get(id1).size == 1 )

    val fields1 = tmpns.nodes.get(id1).get.props
    assert(fields1.size == 4 )
    val titles = Array("ceo", "ui", "dev")
    val salaries = Array(10000, 20000, 30597, 500954)
    val boolattr = Array(false, true, false, true)
    assert(fields1("titles").equals(titles))
    assert(fields1("salaries").equals(salaries))
    assert(fields1("boolattr").equals(boolattr))

  }

}
