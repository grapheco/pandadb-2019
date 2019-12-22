import java.io.File

import cn.pandadb.network.NodeAddress
import cn.pandadb.server.{DataVersionRecoveryArgs, LocalDataVersionRecovery}
import org.junit.{Assert, Test}
import org.neo4j.driver.GraphDatabase

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created at 16:18 2019/12/3
  * @Modified By:
  */

class LocalDataVersionRecoveryTest {

  val localLogFile = new File("./src/test/resources/localLog.json")
  val clusterLogFile = new File("./src/test/resources/clusterLog.json")
  val localNodeAddress = NodeAddress.fromString("159.226.193.204:7687")
  val recoveryArgs = DataVersionRecoveryArgs(localLogFile, clusterLogFile, localNodeAddress)
  val recovery = new LocalDataVersionRecovery(recoveryArgs)
  val driver = GraphDatabase.driver(s"bolt://" + localNodeAddress.getAsString)

  @Test
  def test1(): Unit = {
    val _session = driver.session()
    val beforeResult = _session.run("Match(n) Return(n)")
    Assert.assertEquals(false, beforeResult.hasNext)
    _session.close()
  }

  @Test
  def test2(): Unit = {
    recovery.updateLocalVersion()
    val _session = driver.session()
    val afterResult = _session.run("Match(n) return n;")
    Assert.assertEquals(true, afterResult.hasNext)

    while (afterResult.hasNext) {
      Assert.assertEquals(3.toInt, afterResult.next().get("n").asMap().get("version").toString.toInt)
    }
    _session.run("Match(n) Delete n")
  }

}
