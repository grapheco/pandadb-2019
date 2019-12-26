import java.io.{File, FileInputStream}
import java.util.Properties
import java.util.concurrent.{ExecutorService, Executors}

import cn.pandadb.network.NodeAddress
import cn.pandadb.tool.PNodeServerStarter
import org.junit.Test
import org.neo4j.driver.{Driver, GraphDatabase, StatementResult}

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created at 12:04 2019/12/26
  * @Modified By:
  */
abstract class PandaDBTestBase {
  var serialNum = 0
  val threadPool: ExecutorService = Executors.newFixedThreadPool(3)

//  def startLocalPNodeServer(): NodeAddress = {
//    val startCmd = s"cmd.exe /c mvn exec:java -Dexec.mainClass='cn.pandadb.tool.UnsafePNodeLauncher' -Dexec.args=${serialNum}"
//    startCmd !!;
//    val localNodeAddress = _getLocalNodeAddressFromFile(new File(s"./itest/testdata/localnode${serialNum}.conf"))
//    serialNum += 1;
//    Thread.sleep(10000)
//    localNodeAddress
//  }

  def standAloneLocalPNodeServer(): NodeAddress = {
    threadPool.execute(new UnsafePNodeThread(serialNum))
    print(22222)
    Thread.sleep(10000)
    val localNodeAddress = _getLocalNodeAddressFromFile(new File(s"./testdata/localnode${serialNum}.conf"))
    serialNum += 1
    localNodeAddress
  }

  def executeCypher(driver: Driver, cypher: String): StatementResult = {
    val session = driver.session()
    val tx = session.beginTransaction()
    val result = tx.run(cypher)
    tx.success()
    tx.close()
    session.close()
    result
  }
  // For base test's use.
  private def _getLocalNodeAddressFromFile(confFile: File): NodeAddress = {
    val props = new Properties()
    props.load(new FileInputStream(confFile))
    NodeAddress.fromString(props.getProperty("node.server.address"))
  }

}

object ExampleText extends PandaDBTestBase {
  val driver = GraphDatabase.driver(s"bolt://${standAloneLocalPNodeServer().getAsString}")
}
class ExampleText extends PandaDBTestBase {
  @Test
  def test1(): Unit = {
    val localNodeAddress = standAloneLocalPNodeServer()
    val cypher = ""
    val result = executeCypher(ExampleText.driver, cypher)
  }
}

class UnsafePNodeThread(num: Int) extends Runnable{
  override def run(): Unit = {
    //scalastyle:off
    PNodeServerStarter.main(Array(s"./output/testdb/db${num}",
      s"./testdata/localnode${num}.conf"));
  }
}