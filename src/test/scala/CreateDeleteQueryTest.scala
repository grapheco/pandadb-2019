
import java.io.File

import org.junit.{Before, Test}
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.graphdb.{Label, RelationshipType}
import org.neo4j.io.fs.FileUtils
import org.neo4j.kernel.impl.{CustomPropertyNodeStoreHolder, InMemoryPropertyNodeStore, LoggingPropertiesStore, Settings}


trait CreateDeleteQueryTestBase {
  Settings._hookEnabled = false;

  @Before
  def initdb(): Unit = {
    new File("./output/testdb").mkdirs();
    FileUtils.deleteRecursively(new File("./output/testdb"));
    val db = new GraphDatabaseFactory().newEmbeddedDatabase(new File("./output/testdb"))
    db.shutdown();
  }

  protected def testQuery(query: String): Unit = {
    initdb();
    val db = new GraphDatabaseFactory().newEmbeddedDatabase(new File("./output/testdb"))
    val tx = db.beginTx();
    val rs = db.execute(query);
    while (rs.hasNext) {
      val row = rs.next();
      println(row);
    }

    tx.success();
    db.shutdown();
  }
}

class CreateDeleteNodeQueryTest extends CreateDeleteQueryTestBase {
  Settings._hookEnabled = true;
  CustomPropertyNodeStoreHolder.hold(new LoggingPropertiesStore(new InMemoryPropertyNodeStore()));

  @Test
  def test1(): Unit = {
    testQuery("CREATE (n:Person {name:'test01', age:10})");
    testQuery("CREATE (n:Person {name:'test02', age:20})");
    testQuery("MATCH (n) WHERE 18>n.age RETURN n");
    testQuery("MATCH (n) WHERE 20>=n.age RETURN n");
  }

  @Test
  def test2(): Unit = {
    testQuery("CREATE (n:Person {name:'test01', age:10})");
    testQuery("MATCH (n) WHERE 18>n.age RETURN n");
    testQuery("MATCH (n)  DELETE n");
    testQuery("MATCH (n) WHERE 18>n.age RETURN n");
  }

}
