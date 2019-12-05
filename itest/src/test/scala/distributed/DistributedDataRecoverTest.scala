package distributed

import org.neo4j.driver.GraphDatabase

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created at 17:37 2019/12/4
  * @Modified By:
  */


class DistributedDataRecoverTest {

  val zkString = "10.0.86.26:2181,10.0.86.27:2181,10.0.86.70:2181"
  val pandaString = s"panda://" + zkString
  val driver = GraphDatabase.driver(pandaString)

  def test1(): Unit = {
    val result = driver.session().run("Match(n) Return n;")

  }

}
