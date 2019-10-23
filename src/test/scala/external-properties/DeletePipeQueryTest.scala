
import java.io.File

import org.junit.{Assert, Before, Test}
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.graphdb.{Label, RelationshipType}
import org.neo4j.io.fs.FileUtils
import org.neo4j.kernel.impl.{CustomPropertyNodeStoreHolder, InMemoryPropertyNodeStore, LoggingPropertiesStore, Settings}


trait DeletePipeQueryTestBase {
  Settings._hookEnabled = false;

  @Before
  def initdb(): Unit = {
    new File("./output/testdb").mkdirs();
    FileUtils.deleteRecursively(new File("./output/testdb"));
    val db = new GraphDatabaseFactory().newEmbeddedDatabase(new File("./output/testdb"))

    val tx = db.beginTx();
    //create a node
    val node1 = db.createNode();

    node1.setProperty("name", "test01");
    node1.setProperty("age", 10);
    node1.addLabel(new Label {
      override def name(): String = "man"
    })

    val node2 = db.createNode();
    node2.setProperty("name", "test02");
    node2.setProperty("age", 40);
    node2.addLabel(new Label {
      override def name(): String = "man"
    })
    tx.success()
    tx.close()
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
    tx.close()
    db.shutdown();
  }
}

class DeletePipeQueryTest extends DeletePipeQueryTestBase {
  Settings._hookEnabled = true;
  val tmpns = new InMemoryPropertyNodeStore()
  CustomPropertyNodeStoreHolder.hold(new LoggingPropertiesStore(tmpns));


  @Test
  def test1(): Unit = {

    Assert.assertEquals(2, tmpns.nodes.size)
    testQuery("MATCH (n) WHERE 18>n.age  DELETE n RETURN n.name");
    Assert.assertEquals(1, tmpns.nodes.size)

  }

}
