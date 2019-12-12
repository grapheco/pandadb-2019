package InMemoryPredicatePushDown

import java.io.File
import cn.pandadb.server.PNodeServer
import org.junit.{After, Assert, Before, Test}
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.graphdb.{GraphDatabaseService, Result}
import org.neo4j.io.fs.FileUtils
import cn.pandadb.externalprops.{CustomPropertyNodeStore, InMemoryPropertyNodeStore, InMemoryPropertyNodeStoreFactory}
import cn.pandadb.server.GlobalContext

trait CreateQueryTestBase {
  var db: GraphDatabaseService = null

  @Before
  def initdb(): Unit = {
    PNodeServer.toString
    new File("./output/testdb").mkdirs();
    FileUtils.deleteRecursively(new File("./output/testdb"));
    db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File("./output/testdb")).
      setConfig("external.properties.store.factory", classOf[InMemoryPropertyNodeStoreFactory].getName).
      newGraphDatabase()
    GlobalContext.put(classOf[CustomPropertyNodeStore].getName, InMemoryPropertyNodeStore)

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
    }
    tx.success();
    tx.close()
  }
}

class PredicatePushDown extends CreateQueryTestBase {
  val tmpns = InMemoryPropertyNodeStore

  @Test
  def testLessThan(): Unit = {
    // create one node
    val query = "CREATE (n:Person {age: 10, name: 'bob'})"
    db.execute(query)
    val query2 = "match (n) where 18>n.age return id(n)"
    val rs2 = db.execute(query2)
    var id2: Long = -1
    if (rs2.hasNext) {
      val row = rs2.next()
      id2 = row.get("id(n)").toString.toLong
    }
    assert(id2 != -1)
  }

  @Test
  def testGreaterThan(): Unit = {
    // create one node
    val query = "CREATE (n:Person {age: 10, name: 'bob'})"
    db.execute(query)

    val query3 = "match (n) where 18<n.age return id(n)"
    val rs3 = db.execute(query3)
    var id3: Long = -1
    if (rs3.hasNext) {
      val row = rs3.next()
      id3 = row.get("id(n)").toString.toLong
    }
    assert(id3 == -1)
  }

}