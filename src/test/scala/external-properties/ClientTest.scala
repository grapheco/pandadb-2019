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
    driver.session().run(s"Create(n:TEST{name:'gouhsheng'})")
    driver.session().close()
    val result = driver.session().run("match(n), return n.name")
    Assert.assertEquals("true",result.hasNext)
  }
}
