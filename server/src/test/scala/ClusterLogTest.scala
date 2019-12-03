import java.io.File

import cn.pandadb.server.{DataLogDetail, JsonDataLog}
import com.google.gson.Gson
import org.junit.{Assert, BeforeClass, Test}
/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created at 19:21 2019/12/1
  * @Modified By:
  */
object ClusterLogTest {

  val logFilePath: String = "./datalog.json"
  val logFile = new File(logFilePath)
  @BeforeClass
  def prepareLogFile(): Unit = {
    if (logFile.exists()) {
      logFile.delete()
      logFile.createNewFile()
    }
  }
}


class ClusterLogTest {

  val logFilePath: String = "./datalog.json"
  val logFile = new File(logFilePath)

  val jsonDataLog = new JsonDataLog(logFile)
  val expectedLogArray1: Array[String] = Array("Match(n2), return n2;")
  val expectedLogArray2: Array[String] = Array("Match(n2), return n2;", "Match(n3), return n3;")

  @Test
  def test1(): Unit = {
    jsonDataLog.write(DataLogDetail(100, "Match(n1), return n1;"))
    jsonDataLog.write(DataLogDetail(200, "Match(n2), return n2;"))

  }

  @Test
  def test2(): Unit = {
    val commandList = jsonDataLog.consume(logItem => logItem.command, 150)
    Assert.assertEquals(true, expectedLogArray1.sameElements(commandList))
  }

  @Test
  def test3(): Unit = {
    val jsonDataLog = new JsonDataLog(logFile)
    jsonDataLog.write(DataLogDetail(300, "Match(n3), return n3;"))
    val commandList = jsonDataLog.consume(logItem => logItem.command, 150)
    Assert.assertEquals(true, expectedLogArray2.sameElements(commandList))
  }

}