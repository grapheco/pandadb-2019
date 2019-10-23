
import java.io.File

import org.junit.{Assert, Before, Test}
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.graphdb.{Label, RelationshipType}
import org.neo4j.io.fs.FileUtils
import org.neo4j.kernel.impl.{CustomPropertyNodeStoreHolder, InMemoryPropertyNodeStore, LoggingPropertiesStore, Settings}


trait CreateQueryTestBase {
  Settings._hookEnabled = false;

  @Before
  def initdb(): Unit = {
    new File("./output/testdb").mkdirs();
    FileUtils.deleteRecursively(new File("./output/testdb"));
    val db = new GraphDatabaseFactory().newEmbeddedDatabase(new File("./output/testdb"))
    db.shutdown();
  }

  protected def testQuery(query: String): Unit = {
    val db = new GraphDatabaseFactory().newEmbeddedDatabase(new File("./output/testdb"))
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
  Settings._hookEnabled = true;
  val tmpns = new InMemoryPropertyNodeStore()
  CustomPropertyNodeStoreHolder.hold(new LoggingPropertiesStore(tmpns));

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
