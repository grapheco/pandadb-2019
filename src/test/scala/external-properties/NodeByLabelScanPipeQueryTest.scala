
import java.io.File

import org.junit.{Assert, Before, Test}
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.graphdb.{Label, RelationshipType}
import org.neo4j.io.fs.FileUtils
import org.neo4j.kernel.impl.{CustomPropertyNodeStoreHolder, InMemoryPropertyNodeStore, LoggingPropertiesStore, Settings}


trait NodeByLabelScanPipeQueryTestBase {
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
      override def name(): String = "person"
    })

    val node3 = db.createNode();
    node3.setProperty("name", "test03");
    node3.setProperty("age", 40);
    node3.addLabel(new Label {
      override def name(): String = "person"
    })

    tx.success()
    tx.close()
    db.shutdown();
  }

  protected def testQuery(query: String): Int = {
    initdb();
    val db = new GraphDatabaseFactory().newEmbeddedDatabase(new File("./output/testdb"))
    val tx = db.beginTx();
    val rs = db.execute(query);
    var count = 0
    while (rs.hasNext) {
      count += 1
      val row = rs.next();
      println(row);
    }
    tx.success();
    tx.close()
    db.shutdown();
    count
  }
}

class NodeByLabelScanPipeQueryTest extends NodeByLabelScanPipeQueryTestBase {
  Settings._hookEnabled = true;
  val tmpns = new InMemoryPropertyNodeStore()
  CustomPropertyNodeStoreHolder.hold(new LoggingPropertiesStore(tmpns));


  @Test
  def test1(): Unit = {

    val rows1 = testQuery("MATCH (n: man)  RETURN n.name");
    Assert.assertEquals(1, rows1)

    val rows2 = testQuery("MATCH (n: person)  RETURN n.name");
    Assert.assertEquals(2, rows2)

  }

}
