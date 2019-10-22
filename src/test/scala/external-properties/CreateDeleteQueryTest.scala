
import java.io.File

import org.junit.{Assert, Before, Test}
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
    val tx = db.beginTx();
    //create a node
    val node1 = db.createNode();

    node1.setProperty("name", "lzx1");
    node1.setProperty("age", 20);
    node1.addLabel(new Label {
      override def name(): String = "Person"
    })


    val node2 = db.createNode();
    node2.setProperty("name", "lzx2");
    //with a blob property
    node2.setProperty("age", 10);
    node2.addLabel(new Label {
      override def name(): String = "kid"
    })

    tx.success();
    tx.close();
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
  val tmpns = new InMemoryPropertyNodeStore()
  CustomPropertyNodeStoreHolder.hold(new LoggingPropertiesStore(tmpns));

  @Test
  def test1(): Unit = {
    testQuery("CREATE (n:Person {name:'test01', age:10}) RETURN n.name");
    testQuery("CREATE (n:Person {name:'test02', age:20}) RETURN n.name");
    testQuery("MATCH (n)  RETURN n.name");
  }

  @Test
  def test2(): Unit = {
    testQuery("MATCH (n)  RETURN n.name");

    Assert.assertEquals(2, tmpns.nodes.size)
    testQuery("MATCH (n) WHERE 18>n.age  DELETE n RETURN n.name");
    Assert.assertEquals(1, tmpns.nodes.size)

    testQuery("MATCH (n)  RETURN n.name");
  }

  // test remove labels
  @Test
  def test3(): Unit={
    testQuery("MATCH (n:Person)  RETURN n.name, labels(n)");
    testQuery("MATCH (n:Person) REMOVE n:Person RETURN n.name,labels(n)");
    testQuery("MATCH (n:Person)  RETURN  n.name, labels(n)");
  }

}
