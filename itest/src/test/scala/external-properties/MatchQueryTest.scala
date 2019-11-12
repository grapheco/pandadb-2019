import java.io.File

import org.junit._
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.graphdb.{GraphDatabaseService, Label, RelationshipType, Result}
import org.neo4j.io.fs.FileUtils
import org.neo4j.kernel.impl.{CustomPropertyNodeStoreHolder, InMemoryPropertyNodeStore, LoggingPropertiesStore, Settings}


class MatchQueryTest {
  Settings._hookEnabled = true
  val tmpns = new InMemoryPropertyNodeStore()
  CustomPropertyNodeStoreHolder.hold(new LoggingPropertiesStore(tmpns))

  var db: GraphDatabaseService = null

  @Before
  def initdb(): Unit = {
    new File("./output/testdb").mkdirs()
    FileUtils.deleteRecursively(new File("./output/testdb"))
    db = new GraphDatabaseFactory().newEmbeddedDatabase(new File("./output/testdb"))
    //db.shutdown()
    val tx = db.beginTx()

    val queryStr =
      """
        |CREATE (m1:Movie{title:"Wall Street"}),(m2:Movie{title:"The American President"}),
        |(p1:Person{name: 'Oliver Stone'}),(p2:Person{name: 'Michael Douglas'}),
        |(p3:Person{name: 'Charlie Sheen'}),(p4:Person{name: 'Martin Sheen'}),
        |(p5:Person{name: 'Rob Reiner'})
        |WITH m1,m2,p1,p2,p3,p4,p5
        |CREATE (p1)-[:DIRECTED]->(b)
        |CREATE (p2)-[:ACTED_IN{role: 'Gordon Gekko'}]->(m1)
        |CREATE (p2)-[:ACTED_IN{role: 'President Andrew Shepherd'}]->(m1)
        |CREATE (p3)-[:ACTED_IN{role: 'Bud Fox'}]->(m1)
        |CREATE (p4)-[:ACTED_IN{role: 'Carl Fox'}]->(m1)
        |CREATE (p4)-[:ACTED_IN{role: 'A.J. MacInerney'}]->(m2)
        |CREATE (p5)-[:DIRECTED]->(m2)
        |""".stripMargin
    db.execute(queryStr)
    tx.success()
    tx.close()
  }

  @After
  def closeDb(): Unit = {
    db.shutdown()
  }


  protected def assertResultRowsCount(rowsCount: Int, queryStr: String="match (n) return n"): Unit = {
    // val result = testQuery(queryStr)
    var size = 0
    val tx = db.beginTx()
    val rs = db.execute(queryStr)
    while (rs.hasNext) {
      val row = rs.next()
      println(row)
      size += 1
    }
    tx.success()
    tx.close()
    assert(rowsCount == size)
  }


  @Test
  def test1(): Unit = {
    // Get all nodes
    assertResultRowsCount(6)
  }

  @Test
  def test2(): Unit = {
    // Get all nodes with a label
    assertResultRowsCount(2, "MATCH (movie:Movie) RETURN movie.title")
  }

  @Test
  def test3(): Unit = {
    // Get nodes without label

    //fixme: this will be throw ClassCastException from InMemoryPropertyNodeStore.NFEquals
    assertResultRowsCount(1, "MATCH (movie{title:'Wall Street'}) RETURN movie.title")

  }

  @Test
  def test4(): Unit = {
    // Get nodes with filter

    //fixme: this will be throw ClassCastException from InMemoryPropertyNodeStore.NFEquals
    assertResultRowsCount(1, "MATCH (movie) where movie.title='Wall Street' RETURN movie")
  }



}