
import java.io.File
import org.junit.{After, Before, Test}

import cn.pandadb.server.PNodeServer
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.graphdb.{GraphDatabaseService}
import org.neo4j.io.fs.FileUtils
import org.neo4j.kernel.impl.InMemoryPropertyNodeStoreFactory


trait MatchQueryTestBase {
  var db: GraphDatabaseService = null

  @Before
  def initdb(): Unit = {
    PNodeServer.toString
    new File("./output/testdb").mkdirs();
    FileUtils.deleteRecursively(new File("./output/testdb"));
    db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File("./output/testdb")).
      setConfig("external.properties.store.factory", classOf[InMemoryPropertyNodeStoreFactory].getName).
      newGraphDatabase()
  }

  @After
  def shutdowndb(): Unit = {
    db.shutdown()
  }

  def doCreate(queryStr: String): Unit = {
    val tx = db.beginTx()
    val rs = db.execute(queryStr)
    tx.success()
    tx.close()
  }
}

class MatchQueryTest extends MatchQueryTestBase {

  @Test
  def test1(): Unit = {
    // Get node property

    val query =
      """create (:Person:Man{name:'test01',age:12, born: date('2019-01-02'), array: [1,2,3]})
      """.stripMargin
    doCreate(query)

    val query2 =
      """match (n)
        |return id(n),labels(n), n.name, n.age, n.born, n.array, n.xx
        |""".stripMargin

    val rs = db.execute(query2)
    if (rs.hasNext) {
      val row = rs.next()
      assert(row.get("id(n)").toString.toLong >= 0)

      val labels = row.get("labels(n)").asInstanceOf[java.util.ArrayList[String]]
      assert(labels.size() == 2 && labels.contains("Person") && labels.contains("Man"))

      assert("test01" == row.get("n.name"))
      assert(12 == row.get("n.age"))

      val born = row.get("n.born").asInstanceOf[java.time.LocalDate]
      assert(born.getYear == 2019 && born.getMonth.getValue == 1 && born.getDayOfMonth == 2)

      // TODO: to test ArrayValue, I do not know the type of  <row.get("n.arr")>
      // assert(Array(1,2,3) == row.get("n.arr"))
      assert(row.get("n.array") != null)

      assert(row.get("n.xx") == null)
    }
  }

}
