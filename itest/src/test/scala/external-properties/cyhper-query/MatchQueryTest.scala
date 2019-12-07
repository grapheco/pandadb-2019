
import java.io.File

import org.junit.{Test, Assert}

import org.neo4j.graphdb.Result
import scala.collection.JavaConverters._
import cn.pandadb.server.{GlobalContext, PNodeServer}
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.io.fs.FileUtils
import cn.pandadb.externalprops.{CustomPropertyNodeStore, InMemoryPropertyNodeStore, InMemoryPropertyNodeStoreFactory}


trait MatchQueryTestBase extends QueryTestBase {
  def doCreate(queryStr: String): Unit = {
    val tx = db.beginTx()
    val rs = db.execute(queryStr)
    tx.success()
    tx.close()
  }

  def initData(): Unit = {
    val queryStr =
      """
        |CREATE (n1:Person:Student{name: 'test01',age:15, sex:'male', school: 'No1 Middle School'}),
        |(n2:Person:Teacher{name: 'test02', age: 30, sex:'male', school: 'No1 Middle School', class: 'math'}),
        |(n3:Person:Teacher{name: 'test03', age: 40, sex:'female', school: 'No1 Middle School', class: 'chemistry'})
        |""".stripMargin
    doCreate(queryStr)
  }

  def rsRowCount(rs: Result): Int = {
    var count: Int = 0;
    while (rs.hasNext) {
      count += 1
      println(rs.next())
    }
    return count
  }
}

class MatchQueryTest extends MatchQueryTestBase {

  @Test
  def test1(): Unit = {
    // filter nodes by label

    initData()
    // Get all nodes
    val query1 =
      """match (n) return n
        |""".stripMargin
    val rs = db.execute(query1)
    assert(rsRowCount(rs) == 3)

    // filter by label
    val query2 = "match (n:Person) return n"
    val rs2 = db.execute(query2)
    assert(rsRowCount(rs2) == 3)

    val query3 = "match (n:Teacher) return n"
    val rs3 = db.execute(query3)
    assert(rsRowCount(rs3) == 2)

    val query4 = "match (n:Person:Student) return n"
    val rs4 = db.execute(query4)
    assert(rsRowCount(rs4) == 1)



    return None

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

  @Test
  def test2(): Unit = {
    // filter by property
    initData()

    // filter by {}
    val query1 = "match (n:Person{name: 'test01'}) return n"
    val rs1 = db.execute(query1)
    assert(rsRowCount(rs1) == 1)

    // filter by where
    val query2 = "match (n) where n.name='test01' return n"
    val rs2 = db.execute(query2)
    assert(rsRowCount(rs2) == 1)

    // filter by where
    val query3 = "match (n:Teacher) where n.age<35 and n.sex='male' return n"
    val rs3 = db.execute(query3)
    assert(rsRowCount(rs3) == 1)
  }



}
