
import java.io.File
import java.time.{ZoneId, ZonedDateTime}
import java.util.TimeZone

import scala.collection.mutable
import cn.graiph.server.GNodeServer
import org.junit.{After, Assert, Before, Test}
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.graphdb.{GraphDatabaseService, Result}
import org.neo4j.io.fs.FileUtils
import org.neo4j.kernel.impl.{InMemoryPropertyNodeStore, InMemoryPropertyNodeStoreFactory, Settings}
import org.neo4j.values.storable.{DateTimeValue, DateValue, LocalDateTimeValue, TimeValue}


trait CreateQueryTestBase {
  var db:GraphDatabaseService = null
  @Before
  def initdb(): Unit = {
    GNodeServer.touch()
    new File("./output/testdb").mkdirs();
    FileUtils.deleteRecursively(new File("./output/testdb"));
    db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File("./output/testdb")).
      setConfig("external.properties.store.factory",classOf[InMemoryPropertyNodeStoreFactory].getName).
      newGraphDatabase()
  }

  @After
  def shutdowndb(): Unit = {
    db.shutdown()
  }

  protected def testQuery[T](query: String): Unit = {
    val tx = db.beginTx();
    val rs = db.execute(query);
    while (rs.hasNext) {
      val row = rs.next();
      println(row);
    }
    tx.success();
    tx.close()
  }
}

class CreateNodeQueryTest extends CreateQueryTestBase {
  val tmpns = InMemoryPropertyNodeStore

  @Test
  def test1(): Unit = {
    // create one node
    val tx = db.beginTx()
    val query = "create (n1:Person) return id(n1)"
    val rs = db.execute(query)
    var id1: Long = -1
    if(rs.hasNext){
      val row = rs.next()
      id1 = row.get("id(n1)").toString.toLong
    }
    assert(id1 != -1 )
    assert(tmpns.nodes.size == 1)
    assert(tmpns.nodes.get(id1).get.fields.size == 0)
    assert(tmpns.nodes.get(id1).get.labels.size == 1 && tmpns.nodes.get(id1).get.labels.toList(0) == "Person")
    tx.success()
    tx.close()

    val tx2 = db.beginTx()
    val query2 = "create (n1) return id(n1)"
    val rs2 = db.execute(query2)
    var id2: Long = -1
    if(rs2.hasNext){
      val row = rs2.next()
      id2 = row.get("id(n1)").toString.toLong
    }
    assert(id2 != -1)
    assert(tmpns.nodes.size == 2)
    assert(tmpns.nodes.get(id2).get.fields.size == 0)
    assert(tmpns.nodes.get(id2).get.labels.size == 0)
    tx.success()
    tx.close()
  }

  @Test
  def test2(): Unit = {
    // create multiple nodes
    val tx = db.beginTx()
    val query = "create (n1:Person),(n2:Man) return id(n1),id(n2)"
    val rs = db.execute(query)
    var id1: Long = 0
    var id2: Long = 0
    if(rs.hasNext){
      val row = rs.next()
      id1 = row.get("id(n1)").toString.toLong
      id2 = row.get("id(n2)").toString.toLong
    }
    assert(id1 != -1 && id2 != -1)
    assert(tmpns.nodes.size == 2)
    assert(tmpns.nodes.get(id1).size == 1 && tmpns.nodes.get(id2).size == 1)
    assert(tmpns.nodes.get(id1).get.fields.size == 0 )
    assert(tmpns.nodes.get(id1).get.labels.size == 1 && tmpns.nodes.get(id1).get.labels.toList(0) == "Person")
    assert(tmpns.nodes.get(id2).get.fields.size == 0 )
    assert(tmpns.nodes.get(id2).get.labels.size == 1 && tmpns.nodes.get(id2).get.labels.toList(0) == "Man")
    tx.success()
    tx.close()
  }



  @Test
  def test4(): Unit = {
    // create node with labels and properties
    val tx = db.beginTx()
    val query =
      """CREATE (n1:Person { name:'test01', age:10})
        |CREATE (n2:Person:Man { name:'test02', age:20})
        |RETURN id(n1),id(n2)
      """.stripMargin
    val rs = db.execute(query)
    var id1: Long = -1
    var id2: Long = -1
    if(rs.hasNext){
      val row = rs.next()
      id1 = row.get("id(n1)").toString.toLong
      id2 = row.get("id(n2)").toString.toLong
    }

    assert(tmpns.nodes.get(id1).size == 1 && tmpns.nodes.get(id2).size == 1)

    val fields1 = tmpns.nodes.get(id1).get.fields
    assert(fields1.size == 2 && fields1("name").equals("test01") && fields1("age").equals(10) )
    val labels1 = tmpns.nodes.get(id1).get.labels.toList
    assert(labels1.size == 1 && labels1(0) == "Person")

    val fields2 = tmpns.nodes.get(id2).get.fields
    assert(fields2.size == 2 && fields2("name").equals("test02")  && fields2("age").equals(20) )
    val labels2 = tmpns.nodes.get(id2).get.labels.toList
    assert(labels2.size == 2 && labels2.contains("Person") && labels2.contains("Man") )

    tx.success();
    tx.close()
  }

  @Test
  def test5(): Unit = {
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
    if(rs.hasNext){
      val row = rs.next()
      id1 = row.get("id(n1)").toString.toLong
      id2 = row.get("id(n2)").toString.toLong
      idNeo = row.get("id(neo)").toString.toLong
    }
    assert(tmpns.nodes.size == 3)
    assert(tmpns.nodes.get(id1).size == 1 && tmpns.nodes.get(id2).size == 1 && tmpns.nodes.get(idNeo).size == 1)

    val fields1 = tmpns.nodes.get(id1).get.fields
    assert(fields1.size == 2 && fields1("name").equals("test01") && fields1("age").equals(10) )
    val labels1 = tmpns.nodes.get(id1).get.labels.toList
    assert(labels1.size == 1 && labels1(0) == "Person")

    val fields2 = tmpns.nodes.get(id2).get.fields
    assert(fields2.size == 2 && fields2("name").equals("test02")  && fields2("age").equals(20) )
    val labels2 = tmpns.nodes.get(id2).get.labels.toList
    assert(labels2.size == 1 && labels2.contains("Ceo") )

    val fields3 = tmpns.nodes.get(idNeo).get.fields
    assert(fields3.size == 1 && fields3("business").equals("Software"))
    val labels3 = tmpns.nodes.get(idNeo).get.labels.toList
    assert(labels3.size == 1 && labels3.contains("Company"))
    tx.success();
    tx.close()

  }

  @Test
  def test6(): Unit = {
    // create node with DateTime type property value
    val tx = db.beginTx()
    val query =
      """CREATE (n1:Person { name:'test01',born1:date('2019-01-01'), born2:time('12:05:01')
        |,born3:datetime('2019-01-02T12:05:15[Australia/Eucla]'), born4:datetime('2015-06-24T12:50:35.000000556Z')})
        |RETURN id(n1)
      """.stripMargin
    val rs = db.execute(query)
    var id1: Long = -1
    if(rs.hasNext){
      val row = rs.next()
      id1 = row.get("id(n1)").toString.toLong
    }
    assert(tmpns.nodes.size == 1)
    assert(tmpns.nodes.get(id1).size == 1 )

    val fields1 = tmpns.nodes.get(id1).get.fields
    assert(fields1.size == 5 )
    val born1 = DateValue.date(2019,1,1)
    val born2 = TimeValue.time(12, 5, 1, 0, "Z")
    val born3 = DateTimeValue.datetime(2019,1,2,12,5,15,0,"Australia/Eucla")
    val born4 = DateTimeValue.datetime(2015,6,24,12,50,35,556,ZoneId.of("Z"))
//    val born5 = ZonedDateTime.of(2015,6,24,12,50,35,0,ZoneId.of("Asia/Shanghai"))
    assert(fields1("born1").asInstanceOf[DateValue].equals(born1))
    assert(fields1("born2").asInstanceOf[TimeValue].equals(born2))
    assert(fields1("born3").equals(born3))
    assert(fields1("born4").equals(born4))
    tx.success();
    tx.close()
  }

  @Test
  def test7(): Unit = {
    val born3 = DateTimeValue.datetime(2019,1,2,12,5,15,0,"Australia/Eucla")

    val born2 = TimeValue.time(12, 5, 1, 0, "Z")
    val born4 = DateTimeValue.datetime(2015,6,24,12,50,35,556,"GMT")
    val born5 = DateTimeValue.datetime(2015,6,24,12,50,35,556, "Asia/Shanghai")

    println(born4, born5)
  }

}
