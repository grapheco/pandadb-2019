

package Externalproperties

import java.io.{File, FileInputStream}
import java.util.Properties

import cn.pandadb.externalprops._
import cn.pandadb.server.PNodeServer
import org.junit.{After, Assert, Before, Test}
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.io.fs.FileUtils
import org.neo4j.values.storable.{BooleanArray, LongArray}

/**
  * Created by codeBabyLin on 2019/12/5.
  */
trait QueryTestBase {
  var db: GraphDatabaseService = null
  val nodeStore = "InSolrPropertyNodeStore"

  @Before
  def initdb(): Unit = {
    PNodeServer.toString
    new File("./output/testdb").mkdirs();
    FileUtils.deleteRecursively(new File("./output/testdb"));
    db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File("./output/testdb")).
      newGraphDatabase()
    nodeStore match {
      case "InMemoryPropertyNodeStore" =>
        InMemoryPropertyNodeStore.nodes.clear()
        ExternalPropertiesContext.put(classOf[CustomPropertyNodeStore].getName, InMemoryPropertyNodeStore)

      case "InSolrPropertyNodeStore" =>
        val configFile = new File("./testdata/neo4j.conf")
        val props = new Properties()
        props.load(new FileInputStream(configFile))
        val zkString = props.getProperty("external.properties.store.solr.zk")
        val collectionName = props.getProperty("external.properties.store.solr.collection")
        val solrNodeStore = new InSolrPropertyNodeStore(zkString, collectionName)
        solrNodeStore.clearAll()
        ExternalPropertiesContext.put(classOf[CustomPropertyNodeStore].getName, solrNodeStore)
    }
  }

  @After
  def shutdowndb(): Unit = {
    db.shutdown()
  }

  protected def testQuery[T](query: String): Unit = {
    val tx = db.beginTx();
    val rs = db.execute(query);
    while (rs.hasNext) {
      val row = rs.next();
    }
    tx.success();
    tx.close()
  }
}

trait CreateQueryTestBase extends QueryTestBase {

}

class InSolrArrayTest extends CreateQueryTestBase {

  val configFile = new File("./testdata/neo4j.conf")
  val props = new Properties()
  props.load(new FileInputStream(configFile))
  val zkString = props.getProperty("external.properties.store.solr.zk")
  val collectionName = props.getProperty("external.properties.store.solr.collection")

  //val collectionName = "test"

  //test for node label add and remove
  @Test
  def test1() {
    val solrNodeStore = new InSolrPropertyNodeStore(zkString, collectionName)
    solrNodeStore.clearAll()
    Assert.assertEquals(0, solrNodeStore.getRecorderSize)

    // create node with Array type property value
    val query =
      """CREATE (n1:Person { name:'test01',titles:["ceo","ui","dev"],
        |salaries:[10000,20000,30597,500954], boolattr:[False,True,false,true]})
        |RETURN id(n1)
      """.stripMargin
    val rs = db.execute(query)
    var id1: Long = -1
    if (rs.hasNext) {
      val row = rs.next()
      id1 = row.get("id(n1)").toString.toLong
    }
    Assert.assertEquals(1, solrNodeStore.getRecorderSize)
    val res = solrNodeStore.getNodeById(0)
    val titles = Array("ceo", "ui", "dev")
    val salaries = Array(10000, 20000, 30597, 500954)
    val boolattr = Array(false, true, false, true)

    //scalastyle:off println

    println(res)
    // println(res.get.props.get("titles").get.asObject().getClass)
    // println(res.get.props.get("salaries").get.asObject().getClass)
    // println(res.get.props.get("boolattr").get.asObject().getClass)
    // Assert.assertEquals(3, res.get.props.get("titles").get.asInstanceOf[StringArray].length())
    Assert.assertEquals(4, res.get.props.get("salaries").get.asInstanceOf[LongArray].length())
    Assert.assertEquals(4, res.get.props.get("boolattr").get.asInstanceOf[BooleanArray].length())
    //Assert.assertEquals(boolattr, res.get.props.get("boolattr").get.asObject())
    //assert(res.get.props.get("titles").equals(titles))
    //assert(res.get.props.get("salaries").equals(salaries))
    //assert(res.get.props.get("boolattr").equals(boolattr))

  }

}
