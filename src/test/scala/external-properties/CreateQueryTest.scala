import java.io.File

import org.junit.{After, Before, Test}
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.graphdb.{GraphDatabaseService, Label, RelationshipType, Result}
import org.neo4j.io.fs.FileUtils
import org.neo4j.kernel.impl.{CustomPropertyNodeStoreHolder, InMemoryPropertyNodeStore, LoggingPropertiesStore, Settings}


class CreateQueryTest {
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
  }

  @After
  def closeDb(): Unit = {
    db.shutdown()
  }

  protected def testQuery(queryStr: String): Result = {
    //val db = new GraphDatabaseFactory().newEmbeddedDatabase(new File("./output/testdb"))
    val tx = db.beginTx()
    val rs = db.execute(queryStr)
    tx.success()
    tx.close()
    rs
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
    // create one node
    testQuery("create (n) return n")
    testQuery("create (n) return n")
    assertResultRowsCount(2)
  }

  @Test
  def test2(): Unit = {
    // create multiple nodes
    testQuery("create (n),(m) return n,m")
    testQuery("create (n1),(n2),(n3) return n1,n2,n3")
    assertResultRowsCount(5)
  }

  @Test
  def test3(): Unit = {
    // create node with labels
    testQuery("create (n:Person) return labels(n)")
    testQuery("create (n:Person:Man) return labels(n)")
    testQuery("create (n:Person:Man:Doc) return labels(n)")
    assertResultRowsCount(3, "match (n:Person) return n")
    assertResultRowsCount(2, "match (n:Man) return n")
    assertResultRowsCount(1, "match (n:Doc) return n")
  }

  @Test
  def test4(): Unit = {
    // create node with labels and properties
    testQuery("create (n:Person{name:'andy',age:12}) return n.name,n.age")
    testQuery("create (n:Person{name:'bob',age:12}) return n.name,n.age")
    assertResultRowsCount(2, "match (n:Person) return n")
    assertResultRowsCount(1, "match (n:Person) where n.name='bob' return n")
    assertResultRowsCount(2, "match (n:Person) where n.age=12 return n")
  }

  @Test
  def test5(): Unit = {
    // create relationship
    testQuery("create (n:Person{name:'A',age:12}) return n.name,n.age")
    testQuery("create (n:Person{name:'B',age:12}) return n.name,n.age")
    val queryStr =
      """
        |MATCH (a:Person),(b:Person)
        |WHERE a.name = 'A' AND b.name = 'B'
        |CREATE (a)-[r:RELTYPE]->(b)
        |RETURN type(r)
      """.stripMargin
    assertResultRowsCount(1, queryStr )
  }

  @Test
  def test6(): Unit = {
    // create relationship and set properties
    testQuery("create (n:Person{name:'A',age:12}) return n.name,n.age")
    testQuery("create (n:Person{name:'B',age:10}) return n.name,n.age")
    val queryStr =
      """
        |MATCH (a:Person),(b:Person)
        |WHERE a.name = 'A' AND b.name = 'B'
        |CREATE (a)-[r:RELTYPE { name: "friend"}]->(b)
        |RETURN type(r), r.name
      """.stripMargin
    testQuery(queryStr)
    assertResultRowsCount(1, "match (a)-[r:RELTYPE { name: 'friend'}]->(b) return type(r), r.name" )
  }

  @Test
  def test7(): Unit = {
    // Create a full path
    val queryStr =
      """
        |CREATE p =(andy { name:'Andy' })-[:WORKS_AT]->(neo)<-[:WORKS_AT]-(michael { name: 'Michael' })
        |RETURN p
      """.stripMargin
    assertResultRowsCount(1, queryStr )
  }

}