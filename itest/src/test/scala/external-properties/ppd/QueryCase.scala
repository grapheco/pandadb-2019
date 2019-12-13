package ppd

import java.io.File

import cn.pandadb.externalprops.{CustomPropertyNodeStore, InMemoryPropertyNodeStore, InMemoryPropertyNodeStoreFactory}
import cn.pandadb.server.{GlobalContext, PNodeServer}
import org.junit.{After, Before, Test}
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.io.fs.FileUtils

trait QueryCase {

  var db: GraphDatabaseService = null

  @Before
  def initdb(): Unit

  @After
  def shutdowndb(): Unit = {
    db.shutdown()
  }

  @Test
  def testLessThan(): Unit = {
    // create one node
    val query = "CREATE (n:Person {age: 10, name: 'bob'})"
    db.execute(query)
    val query2 = "match (n) where 18>n.age return id(n)"
    val rs2 = db.execute(query2)
    var id2: Long = -1
    if (rs2.hasNext) {
      val row = rs2.next()
      id2 = row.get("id(n)").toString.toLong
    }
    assert(id2 != -1)
  }

  @Test
  def testGreaterThan(): Unit = {
    // create one node
    val query = "CREATE (n:Person {age: 10, name: 'bob'})"
    db.execute(query)

    val query3 = "match (n) where 18<n.age return id(n)"
    val rs3 = db.execute(query3)
    var id3: Long = -1
    if (rs3.hasNext) {
      val row = rs3.next()
      id3 = row.get("id(n)").toString.toLong
    }
    assert(id3 == -1)
  }

  @Test
  def testEqual(): Unit = {
    // create one node
    val query = "CREATE (n:Person {age: 10, name: 'bob'})"
    db.execute(query)
    val query2 = "match (n) where n.name = 'bob' return id(n)"
    val rs2 = db.execute(query2)
    var id2: Long = -1
    if (rs2.hasNext) {
      val row = rs2.next()
      id2 = row.get("id(n)").toString.toLong
    }
    assert(id2 != -1)
  }

  @Test
  def testEndsWith(): Unit = {
    // create one node
    val query = "CREATE (n:Person {age: 10, name: 'bob'})"
    db.execute(query)

    val query3 = "match (n) where n.name ENDS WITH 'a' return id(n)"
    val rs3 = db.execute(query3)
    var id3: Long = -1
    if (rs3.hasNext) {
      val row = rs3.next()
      id3 = row.get("id(n)").toString.toLong
    }
    assert(id3 == -1)
  }

  @Test
  def testLableAndEndsWith(): Unit = {
    // create one node
    val query = "CREATE (n:Person {age: 10, name: 'bob'})"
    db.execute(query)

    val query3 = "match (n:Person) where n.name ENDS WITH 'a' return id(n)"
    val rs3 = db.execute(query3)
    var id3: Long = -1
    if (rs3.hasNext) {
      val row = rs3.next()
      id3 = row.get("id(n)").toString.toLong
    }
    assert(id3 == -1)
  }

  @Test
  def testJoin(): Unit = {
    // create one node
    val query = "CREATE (n:Person {age: 10, name: 'bob'})"
    db.execute(query)

    val query3 = "Match p=()--() return count(p)"
    val rs3 = db.execute(query3)
    var cnt: Long = -1
    if (rs3.hasNext) {
      val row = rs3.next()
      cnt = row.get("count(p)").toString.toLong
    }
    assert(cnt == 0)
  }

}
