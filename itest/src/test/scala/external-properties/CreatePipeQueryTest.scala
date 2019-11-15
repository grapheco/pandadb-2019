
import java.io.File

import cn.graiph.server.GNodeServer
import org.junit.{Assert, Before, Test}
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.graphdb.{Label, RelationshipType}
import org.neo4j.io.fs.FileUtils
import org.neo4j.kernel.impl.{InMemoryPropertyNodeStoreFactory, InMemoryPropertyNodeStore, Settings}


trait CreateQueryTestBase {

  @Before
  def initdb(): Unit = {
    GNodeServer.touch()
    new File("./output/testdb").mkdirs();
    FileUtils.deleteRecursively(new File("./output/testdb"));
    val db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File("./output/testdb")).
      setConfig("external.properties.store.factory",classOf[InMemoryPropertyNodeStoreFactory].getName).
      newGraphDatabase()
    db.shutdown();
  }

  protected def testQuery(query: String): Unit = {
    val db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File("./output/testdb")).
      setConfig("external.properties.store.factory",classOf[InMemoryPropertyNodeStoreFactory].getName).
      newGraphDatabase()
    val tx = db.beginTx();
    val rs = db.execute(query);
    while (rs.hasNext) {
      val row = rs.next();
      println(row);
    }
    tx.success();
    tx.close()
    db.shutdown();
  }
}

class CreateNodeQueryTest extends CreateQueryTestBase {
  val tmpns = InMemoryPropertyNodeStore

  @Test
  def test1(): Unit = {
    Assert.assertEquals(0, tmpns.nodes.size)
    testQuery("CREATE (n:Person { name:'test01', age:10}) RETURN n.name, id(n)");
    testQuery("CREATE (n:Person { name:'test02', age:20}) RETURN n.name, id(n)");
    testQuery("CREATE (n:Person { name:'test03', age:20}) RETURN n.name, id(n)");
    testQuery("MATCH (n)  RETURN n.name");
    Assert.assertEquals(3, tmpns.nodes.size)
  }
}
