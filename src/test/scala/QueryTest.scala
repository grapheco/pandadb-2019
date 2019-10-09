/**
  * Created by bluejoe on 2019/9/15.
  */

import java.io.File

import org.junit.{Before, Test}
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.graphdb.{Label, RelationshipType}
import org.neo4j.io.fs.FileUtils
import org.neo4j.kernel.impl.{CustomPropertyNodeStoreHolder, InMemoryPropertyNodeStore, LoggingPropertiesStore, Settings}

class QueryTest {
  @Before
  def setup(): Unit = {
    new File("./output/testdb").mkdirs();
    FileUtils.deleteRecursively(new File("./output/testdb"));
    val db = new GraphDatabaseFactory().newEmbeddedDatabase(new File("./output/testdb"))

    val tx = db.beginTx();
    //create a node
    val node1 = db.createNode();

    node1.setProperty("name", "bluejoe");
    node1.setProperty("age", 40);
    node1.addLabel(new Label {
      override def name(): String = "person"
    })

    val node2 = db.createNode();
    node2.setProperty("name", "alex");
    //with a blob property
    node2.setProperty("age", 10);
    node2.addLabel(new Label {
      override def name(): String = "person"
    })

    node2.createRelationshipTo(node1, new RelationshipType {
      override def name(): String = "dad"
    });

    tx.success();
    tx.close();
    db.shutdown();
  }

  @Test
  def test1(): Unit = {
    Settings._hook_enabled = false;
    _test();
  }

  @Test
  def test2(): Unit = {
    Settings._hook_enabled = true;
    CustomPropertyNodeStoreHolder.hold(new LoggingPropertiesStore(new InMemoryPropertyNodeStore()));
    _test();
  }

  private def _test(): Unit = {
    val db = new GraphDatabaseFactory().newEmbeddedDatabase(new File("./output/testdb"))

    val tx = db.beginTx();
    val nodes = db.getAllNodes.iterator();
    while (nodes.hasNext) {
      println(nodes.next());
    }
    //org.neo4j.cypher.internal.javacompat.ExecutionEngine
    //org.neo4j.cypher.internal.PreParser
    //org.neo4j.cypher.internal.MasterCompiler
    //org.neo4j.cypher.internal.compatibility.v3_5.Cypher35Planner
    val rs = db.execute("match (m)-[dad]->(n) where 18>m.age return n.name, m");
    while (rs.hasNext) {
      val row = rs.next();
      println(row);
    }

    tx.success();
    db.shutdown();
  }

}
