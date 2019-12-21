package distributed

import org.junit.Test
import org.neo4j.driver.{AuthTokens, Driver, GraphDatabase, StatementResult}

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created at 13:41 2019/11/30
  * @Modified By:
  */
class WriteTest {
  val pandaDriver: Driver = GraphDatabase.driver("panda://10.0.86.26:2181/db", AuthTokens.basic("", ""))
  val cypherList = List("Create(n:Test{name:'alice'});", "Create(n:Test{age:10});", "Merge(n:Test{name:'alice'});")
//
  @Test
  def test1(): Unit = {
    val taskList: ListBuffer[Future[StatementResult]] = new ListBuffer[Future[StatementResult]]
    cypherList.foreach(cypher => {
      execute(pandaDriver, cypher)
    })

  }

  def execute(driver: Driver, cypher: String): StatementResult = {
    val session = driver.session()
    val tx = session.beginTransaction()
    val statementResult = tx.run(cypher)
    tx.success()
    tx.close()
    session.close()
    statementResult
  }
}
