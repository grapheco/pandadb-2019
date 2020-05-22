
import java.io.File

import scala.collection.JavaConverters._
import org.junit.{After, Before, Test}
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.graphdb.{GraphDatabaseService, Label, RelationshipType}
import org.neo4j.io.fs.FileUtils

class HackingEnityIdTests {

  var db: GraphDatabaseService = null

  @Before
  def initDB(): Unit = {
    val dbFile = new File("./output/testdb")
    if (dbFile.exists()) {
      FileUtils.deleteRecursively(new File("./output/testdb"))
    }
    else {
      dbFile.mkdirs()
    }
    db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbFile).
      newGraphDatabase()
  }

  @After
  def shutdowndb(): Unit = {
    db.shutdown()
  }

  @Test
  def testForCreateNodeAndGetById(): Unit = {
    val tx = db.beginTx()
    val node = db.createNode(5);
    assert(node.getId == 5)
    tx.success()
    tx.close()

    val tx2 = db.beginTx()
    val node2 = db.getNodeById(5)
    assert(node2!=null && node2.getId==5)
    tx2.close()
  }

  @Test
  def testForCreateNodesAndGetAll(): Unit = {
    val nodeIds = Array(10, 20, 30, 40)
    val tx = db.beginTx()
    for (id <- nodeIds) {
      db.createNode(id)
    }
    tx.success()
    tx.close()

    val tx2 = db.beginTx()
    val nodes = db.getAllNodes.asScala
    assert(nodes.size == 4)
    for(n <- nodes) {
      nodeIds.contains(n.getId)
    }
    tx2.close()
  }

  @Test
  def testForCreateNodeWithLabels(): Unit = {
    val tx = db.beginTx()
    val label1 = Label.label("label1")
    val label2 = Label.label("label2")
    val label3 = Label.label("label3")
    val node = db.createNode(10, label1, label2, label3);
    assert(node.getId == 10)
    tx.success()
    tx.close()

    val tx2 = db.beginTx()
    val node2 = db.getNodeById(10)
    assert(node2!=null && node2.getId==10)
    val labels = node2.getLabels.asScala
    for(label <- labels) {
      val labelName = label.name()
      assert(labelName == "label1" || labelName == "label2" || labelName == "label3")
    }
    tx2.close()
  }

  @Test
  def testForCreateRelationshipAndGetById(): Unit = {
    val tx1 = db.beginTx()
    val node1 = db.createNode(3, Label.label("person"))
    node1.setProperty("name", "test1")
    val node2 = db.createNode(5, Label.label("person"))
    node2.setProperty("name", "test2")
    val node3 = db.createNode(9, Label.label("person"))
    node3.setProperty("name", "test3")

    val relType = RelationshipType.withName("friend")
    val rel1Id = 10
    val rel2Id = 20

    node1.createRelationshipTo(node2, relType, rel1Id)
    node1.createRelationshipTo(node3, relType, rel2Id)

    tx1.success()
    tx1.close()

    val tx2 = db.beginTx()
    val rel3 = db.getRelationshipById(rel1Id)
    val rel4 = db.getRelationshipById(rel2Id)
    assert(rel3.getId == rel1Id && rel3.getStartNodeId == node1.getId && rel3.getEndNodeId == node2.getId)
    assert(rel4.getId == rel2Id && rel4.getStartNodeId == node1.getId && rel4.getEndNodeId == node3.getId)
    tx2.close()
  }

  @Test
  def testForCreateRelationshipAndGetAll(): Unit = {
    val tx1 = db.beginTx()
    val node1 = db.createNode(3, Label.label("person"))
    node1.setProperty("name", "test1")
    val node2 = db.createNode(5, Label.label("person"))
    node2.setProperty("name", "test2")
    val node3 = db.createNode(9, Label.label("person"))
    node3.setProperty("name", "test3")

    val relType1 = RelationshipType.withName("friend")
    val relType2 = RelationshipType.withName("work")

    node1.createRelationshipTo(node2, relType1, 10)
    node1.createRelationshipTo(node3, relType1, 20)
    node2.createRelationshipTo(node3, relType2, 55)
    tx1.success()
    tx1.close()

    val tx2 = db.beginTx()
    val relationships = db.getAllRelationships.asScala
    assert(relationships.size == 3)
    for (r <- relationships) {
      r.getId match {
        case 10 => assert(r.getStartNodeId==3 && r.getEndNodeId==5)
        case 20 => assert(r.getStartNodeId==3 && r.getEndNodeId==9)
        case 55 => assert(r.getStartNodeId==5 && r.getEndNodeId==9)
        case _ => assert(false)
      }
    }
    tx2.close()
  }
}