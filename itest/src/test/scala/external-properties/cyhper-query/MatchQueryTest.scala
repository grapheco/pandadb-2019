
import org.junit.Test
import org.neo4j.graphdb.Result


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
        | """.stripMargin
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
        | """.stripMargin
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

  @Test
  def test3(): Unit = {
    // get property
    initData()

    // filter by {}
    val query1 = "match (n:Person{name: 'test01'}) return n.sex, n.age, n.class, n.school"
    val rs1 = db.execute(query1)
    assert(rsRowCount(rs1) == 1)
    while (rs1.hasNext) {
      val row = rs1.next()
      val sex = row.get("n.sex")
      assert("male" == sex)
      val age = row.get("n.age")
      assert(15 == age)
      val school = row.get("n.school")
      assert("No1 Middle School" == school)
    }
  }


}
