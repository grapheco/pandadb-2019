package InSolrPredicatePushDown

import java.io.{File, FileInputStream}
import java.util.Properties

import cn.pandadb.server.PNodeServer
import org.junit.{After, Assert, Before, Test}
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.graphdb.{GraphDatabaseService, Result}
import org.neo4j.io.fs.FileUtils
import cn.pandadb.externalprops.{CustomPropertyNodeStore, InSolrPropertyNodeStore, InSolrPropertyNodeStoreFactory}
import cn.pandadb.server.GlobalContext

trait CreateQueryTestBase1 {
  var db: GraphDatabaseService = null

  @Before
  def initdb(): Unit = {
    PNodeServer.toString
    new File("./output/testdb").mkdirs();
    FileUtils.deleteRecursively(new File("./output/testdb"));
    db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File("./output/testdb")).
      newGraphDatabase()

    val configFile = new File("./testdata/neo4j.conf")
    val props = new Properties()
    props.load(new FileInputStream(configFile))
    val zkString = props.getProperty("external.properties.store.solr.zk")
    val collectionName = props.getProperty("external.properties.store.solr.collection")
    val solrNodeStore = new InSolrPropertyNodeStore(zkString, collectionName)
    solrNodeStore.clearAll()
    GlobalContext.put(classOf[CustomPropertyNodeStore].getName, solrNodeStore)

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

class PredicatePushDown extends CreateQueryTestBase1 {

  @Test
  def testEqual(): Unit = {
    // create one node
    val query = "CREATE (n:Person {age: 10, name: 'bob'})"
    db.execute(query)
    val query2 = "match (n) where n.name = 'bob' return id(n)"
    val rs2 = db.execute(query2)
    var id2: Long = -1
    if (rs2.hasNext) {
      val row = rs2.next()
      id2 = row.get("id(n)").toString.toLong
    }
    assert(id2 != -1)
  }

  @Test
  def testEndsWith(): Unit = {
    // create one node
    val query = "CREATE (n:Person {age: 10, name: 'bob'})"
    db.execute(query)

    val query3 = "match (n) where n.name ENDS WITH 'a' return id(n)"
    val rs3 = db.execute(query3)
    var id3: Long = -1
    if (rs3.hasNext) {
      val row = rs3.next()
      id3 = row.get("id(n)").toString.toLong
    }
    assert(id3 == -1)
  }

}