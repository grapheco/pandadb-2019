package cn.pandadb.tests

import java.io.File

import scala.collection.JavaConverters._
import cn.pandadb.configuration.Config
import cn.pandadb.datanode.DataNodeTxServiceImpl
import org.junit.{Before, Test, After}
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.kernel.impl.coreapi.TopLevelTransaction
import org.neo4j.io.fs.FileUtils

class DataNodeTxServiceTests {
  var db: GraphDatabaseService = null
  var txService: DataNodeTxServiceImpl = null

  @Before
  def init(): Unit = {
    val dbFile = new File("output/graph.db")
    if (dbFile.exists()) {
      FileUtils.deleteRecursively(dbFile)
    }
   db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File("output/graph.db")).newGraphDatabase()
   txService = new DataNodeTxServiceImpl(db, new Config())
  }

  @After
  def shutdowndb(): Unit = {
    db.shutdown()
  }

  @Test
  def testBeginCloseTx(): Unit = {
    val txId = System.currentTimeMillis()
    val tx = txService.beginTx(txId)
    assert(tx.isInstanceOf[TopLevelTransaction])
    val commitRes = txService.commitTx(txId)
    assert(commitRes.equals(true))
    val closeRes = txService.closeTx(txId)
    assert(closeRes.equals(true))
  }

  @Test
  def testSuccessCreateNodeInTx(): Unit = {
    val txId = System.currentTimeMillis()
    val tx = txService.beginTx(txId)

    val node1 = txService.createNodeInTx(txId, 1L, Array("person", "boy"), Map[String, Any]("name"->"test1", "age"->10))
    assert(node1.id.equals(1L) && node1.props("name").asString().equals("test1"))
    val node2 = txService.createNodeInTx(txId, 2L, Array("person", "boy"), Map[String, Any]("name"->"test2", "age"->12))
    assert(node2.id.equals(2L) && node2.props("name").asString().equals("test2"))

    var dbTx = db.beginTx()
    var dbNodes = db.getAllNodes().iterator().asScala.toList
    assert(dbNodes.size.equals(0))
    dbTx.close()

    val commitRes = txService.commitTx(txId, true)
    val closeRes = txService.closeTx(txId)
    assert(closeRes.equals(true))

    dbTx = db.beginTx()
    dbNodes = db.getAllNodes().iterator().asScala.toList
    assert(dbNodes.size.equals(2))
    dbTx.close()
  }

  @Test
  def testFailCreateNodeInTx2(): Unit = {
    val txId = System.currentTimeMillis()
    val tx = txService.beginTx(txId)

    val node1 = txService.createNodeInTx(txId, 1L, Array("person", "boy"), Map[String, Any]("name"->"test1", "age"->10))
    assert(node1.id.equals(1L) && node1.props("name").asString().equals("test1"))
    val node2 = txService.createNodeInTx(txId, 2L, Array("person", "boy"), Map[String, Any]("name"->"test2", "age"->12))
    assert(node2.id.equals(2L) && node2.props("name").asString().equals("test2"))

    var dbTx = db.beginTx()
    var dbNodes = db.getAllNodes().iterator().asScala.toList
    assert(dbNodes.size.equals(0))
    dbTx.close()

    val commitRes = txService.commitTx(txId, false)
    val closeRes = txService.closeTx(txId)
    assert(closeRes.equals(true))

    dbTx = db.beginTx()
    dbNodes = db.getAllNodes().iterator().asScala.toList
    assert(dbNodes.size.equals(0))
    dbTx.close()
  }
}
