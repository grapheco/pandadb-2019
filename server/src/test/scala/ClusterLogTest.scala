import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}

import cn.pandadb.server.{DataLogDetail, JsonDataLogRW}
import org.junit.{Assert, BeforeClass, Test}
import ClusterLogTest.logFile
/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created at 19:21 2019/12/1
  * @Modified By:
  */
object ClusterLogTest {

  val testdataPath: String = "./src/test/testdata/";
  val logFilePath: String = "./src/test/testdata/datalog.json"
  val logFile = new File(logFilePath)
  @BeforeClass
  def prepareLogFile(): Unit = {
    if (logFile.exists()) {
      logFile.delete()
      logFile.createNewFile()
    } else {
      new File(testdataPath).mkdirs()
      logFile.createNewFile()
    }
  }
}

class ClusterLogTest {

  val expectedLogArray1: Array[String] = Array("Match(n2), return n2;")
  val expectedLogArray2: Array[String] = Array("Match(n2), return n2;", "Match(n3), return n3;")

  @Test
  def test1(): Unit = {
    val jsonDataLogRW = new JsonDataLogRW(logFile)
    Assert.assertEquals(0, logFile.length())
    jsonDataLogRW.write(DataLogDetail(100, "Match(n1), return n1;"))
    jsonDataLogRW.write(DataLogDetail(200, "Match(n2), return n2;"))
    val _bf = new BufferedReader(new InputStreamReader(new FileInputStream(logFile)))
    Assert.assertEquals(s"""{"versionNum":${100},"command":"Match(n1), return n1;"}""", _bf.readLine())
    Assert.assertEquals(s"""{"versionNum":${200},"command":"Match(n2), return n2;"}""", _bf.readLine())
  }

  @Test
  def test2(): Unit = {
    val jsonDataLogRW = new JsonDataLogRW(logFile)
    val commandList = jsonDataLogRW.consume(logItem => logItem.command, 150)
    Assert.assertEquals(true, expectedLogArray1.sameElements(commandList))
  }

  @Test
  def test3(): Unit = {
    val jsonDataLogRW = new JsonDataLogRW(logFile)
    val jsonDataLog = new JsonDataLogRW(logFile)
    jsonDataLog.write(DataLogDetail(300, "Match(n3), return n3;"))
    val commandList = jsonDataLog.consume(logItem => logItem.command, 150)
    Assert.assertEquals(true, expectedLogArray2.sameElements(commandList))
  }
}