
import java.io.File

import cn.pandadb.server.GNodeServer
import org.junit.{After, Before, Test}
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.io.fs.FileUtils
import org.neo4j.kernel.impl.{InMemoryPropertyNodeStore, InMemoryPropertyNodeStoreFactory}


trait UpdateQueryTestBase {
  var db: GraphDatabaseService = null

  @Before
  def initdb(): Unit = {
    GNodeServer.touch()
    new File("./output/testdb").mkdirs();
    FileUtils.deleteRecursively(new File("./output/testdb"));
    db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File("./output/testdb")).
      setConfig("external.properties.store.factory", classOf[InMemoryPropertyNodeStoreFactory].getName).
      newGraphDatabase()
  }

  @After
  def shutdowndb(): Unit = {
    db.shutdown()
  }

}

class UpdateNodeQueryTest extends UpdateQueryTestBase {
  val tmpns = InMemoryPropertyNodeStore

  @Test
  def test1(): Unit = {
    // update and add node properties

    // create node
    val tx = db.beginTx()
    val query = "create (n1:Person) return id(n1)"
    val rs = db.execute(query)
    var id1: Long = -1
    if(rs.hasNext) {
      val row = rs.next()
      id1 = row.get("id(n1)").toString.toLong
    }
    tx.success()
    tx.close()
    assert(id1 != -1 )
    assert(tmpns.nodes.size == 1)
    assert(tmpns.nodes.get(id1).get.fields.size == 0)
    assert(tmpns.nodes.get(id1).get.labels.size == 1 && tmpns.nodes.get(id1).get.labels.toList(0) == "Person")

    // update and add properties
    val tx2 = db.beginTx()
    val query2 = s"match (n1:Person) where id(n1)=$id1 set n1.name='test01', n1.age=10 return n1.name,n1.age"
    db.execute(query2)
    tx2.success()
    tx2.close()
    assert(tmpns.nodes.size == 1)
    val fields = tmpns.nodes.get(id1).get.fields
    assert(fields.size == 2 && fields("name").equals("test01") && fields("age").equals(10)  )
  }

  @Test
  def test2(): Unit = {
    // add labels

    // create node
    val tx = db.beginTx()
    val query = "create (n1:Person{name:'xx'}) return id(n1)"
    val rs = db.execute(query)
    var id1: Long = -1
    if(rs.hasNext){
      val row = rs.next()
      id1 = row.get("id(n1)").toString.toLong
    }
    tx.success()
    tx.close()
    assert(id1 != -1 )
    assert(tmpns.nodes.size == 1)
    assert(tmpns.nodes.get(id1).get.fields.size == 1)
    assert(tmpns.nodes.get(id1).get.labels.size == 1 && tmpns.nodes.get(id1).get.labels.toList(0) == "Person")

    // add labels
    val tx3 = db.beginTx()
    val query3 = s"match (n1:Person) where id(n1)=$id1 set n1:Man:Boy:Person return labels(n1)"
    val rs3 = db.execute(query3)
    tx3.success()
    tx3.close()
    assert(tmpns.nodes.size == 1)
    val labels = tmpns.nodes.get(id1).get.labels.toList
    assert(labels.size == 3 && labels.contains("Person") && labels.contains("Man") && labels.contains("Boy") )
  }





}
