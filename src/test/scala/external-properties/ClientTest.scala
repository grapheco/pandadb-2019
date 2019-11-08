import org.junit.{Assert, Test}
import org.neo4j.driver.{AuthTokens, Driver, GraphDatabase}

class ClientTest {

  val coorUrl=s"bolt://159.226.193.204:7685";

  def connectCoordinator(coorUrl: String): Driver = {
    val driver = GraphDatabase.driver(coorUrl,AuthTokens.basic("", ""))
    driver
  }


  @Test
  def create(): Unit ={
    val driver = connectCoordinator(coorUrl)
    val session = driver.session()
    val writeTX = session.beginTransaction()
    writeTX.run(s"Create(n:TEST{name:'gouhsheng'})")
    writeTX.success()
    session.close()
  }

  @Test
  def matchAll(): Unit ={
    val driver = connectCoordinator(coorUrl)
    val session = driver.session()
    val resultSet = session.run(s"match (n) return n.name, n.age, n.isClever")
    val record = resultSet.next()
    println(record)
  }

  @Test
  def sickBlobTest(): Unit ={
    val driver = connectCoordinator(coorUrl)
    val session = driver.session()
    val resultSet = session.run(s"return <https://avatars0.githubusercontent.com/u/2328905?s=460&v=4>")
    val record = resultSet.next()
    println(record)
  }


}
