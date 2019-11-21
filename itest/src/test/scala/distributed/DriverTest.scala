package distributed

import org.junit.{Assert, Test}
import org.neo4j.driver.{AuthTokens, GraphDatabase}

/**
  * Created by bluejoe on 2019/11/21.
  */
class DriverTest {
  @Test
  def test1() {
    val driver = GraphDatabase.driver("panda://10.0.86.179:2181,10.0.87.45:2181,10.0.87.46:2181/db1", AuthTokens.basic("", ""));
    val session = driver.session();
    val results = session.run("match (n) return n.name");
    val result = results.next();
    Assert.assertEquals("bluejoe", result.get("n.name").asString());
    session.close();
    driver.close();
  }
}
