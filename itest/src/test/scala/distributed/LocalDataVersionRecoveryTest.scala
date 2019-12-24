package distributed

import java.io.{File, FileInputStream}
import java.util.Properties
import java.util.concurrent.{ExecutorService, Executors}

import cn.pandadb.network.NodeAddress
import cn.pandadb.server.{DataVersionRecoveryArgs, LocalDataVersionRecovery}
import distributed.LocalDataVersionRecoveryTest.{neodriver, recovery}
import org.junit.{Assert, BeforeClass, Test}
import org.neo4j.driver.{Driver, GraphDatabase}

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created at 16:18 2019/12/3
  * @Modified By:
  */
object LocalDataVersionRecoveryTest {

  val localLogFile = new File("./src/test/resources/localLog.json")
  val clusterLogFile = new File("./src/test/resources/clusterLog.json")

  val localPNodeServer = new LocalServerThread(0)

  val confFile = new File("../itest/testdata/localnode0.conf")
  val props = new Properties()
  props.load(new FileInputStream(confFile))
  val localNodeAddress = NodeAddress.fromString(props.getProperty("node.server.address"))

  val recoveryArgs = DataVersionRecoveryArgs(localLogFile, clusterLogFile, localNodeAddress)
  val recovery = new LocalDataVersionRecovery(recoveryArgs)

  val neodriver: Driver = {
    GraphDatabase.driver(s"bolt://" + localNodeAddress.getAsString)
  }

  @BeforeClass
  private def _startServer(): Unit = {
    val threadPool: ExecutorService = Executors.newFixedThreadPool(1)
    threadPool.execute(localPNodeServer)
    Thread.sleep(10000)
  }
  _startServer()

}

class LocalDataVersionRecoveryTest {

  @Test
  def test1(): Unit = {
    val _session = neodriver.session()
    val beforeResult = _session.run("Match(n) Return(n)")
    Assert.assertEquals(false, beforeResult.hasNext)
    _session.close()
  }

  @Test
  def test2(): Unit = {
    recovery.updateLocalVersion()
    val _session = neodriver.session()
    val afterResult = _session.run("Match(n) return n;")
    Assert.assertEquals(true, afterResult.hasNext)

    while (afterResult.hasNext) {
      Assert.assertEquals(3.toInt, afterResult.next().get("n").asMap().get("version").toString.toInt)
    }
    _session.run("Match(n) Delete n")
  }

}
