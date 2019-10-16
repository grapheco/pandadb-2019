/**
  * Created by bluejoe on 2019/9/15.
  */

import java.io.File

import org.junit.{Before, Test}
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.graphdb.{Label, RelationshipType}
import org.neo4j.io.fs.FileUtils
import org.neo4j.kernel.impl.{CustomPropertyNodeStoreHolder, InMemoryPropertyNodeStore, LoggingPropertiesStore, Settings}


class QueryTest extends QueryTestBase {
  Settings._hookEnabled = false;

  @Test
  def test1(): Unit = {
    testQuery("match (m)-[dad]->(n) where 18>m.age return n.name, m");
  }

  @Test
  def test2(): Unit = {
    testQuery("match (m)-[dad]->(x)-[brother]-(n) where m.age<18 and n.age>30 return n.name, m.name, x");
  }
}

class QueryWithinSolrTest extends QueryTestBase {
  Settings._hookEnabled = true;
  CustomPropertyNodeStoreHolder.hold(new LoggingPropertiesStore(new InMemoryPropertyNodeStore()));

  @Test
  def test1(): Unit = {
    testQuery("match (m)-[dad]->(n) where 18>m.age return n.name, m");
  }

  // test beforeCommit update nodes
  @Test
  def test2(): Unit = {
    testQuery("match (n) return n.name")
}

trait QueryTestBase {
  Settings._hookEnabled = false;

  @Before
  def initdb(): Unit = {
    new File("./output/testdb").mkdirs();
    FileUtils.deleteRecursively(new File("./output/testdb"));
    val db = new GraphDatabaseFactory().newEmbeddedDatabase(new File("./output/testdb"))

    val tx = db.beginTx();
    //create a node
    val node1 = db.createNode();

    node1.setProperty("name", "bluejoe");
    node1.setProperty("age", 40);
    node1.addLabel(new Label {
      override def name(): String = "man"
    })

    val node2 = db.createNode();
    node2.setProperty("name", "alex");
    //with a blob property
    node2.setProperty("age", 10);
    node2.addLabel(new Label {
      override def name(): String = "kid"
    })

    val node3 = db.createNode();

    node3.setProperty("name", "alan");
    node3.setProperty("age", 39);
    node3.addLabel(new Label {
      override def name(): String = "man"
    })

    node2.createRelationshipTo(node1, new RelationshipType {
      override def name(): String = "dad"
    });

    node3.createRelationshipTo(node1, new RelationshipType {
      override def name(): String = "brother"
    });

    // test update properties
    val node4 = db.createNode()
    node4.setProperty("name", "test")
    node4.setProperty("name", "update test name")

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

