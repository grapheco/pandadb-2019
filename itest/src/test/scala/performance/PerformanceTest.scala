package performance

import java.io.{File, FileInputStream, PrintWriter}
import java.util.Properties
import java.util.concurrent.TimeoutException

import com.google.gson.GsonBuilder
import org.junit.Test
import org.neo4j.driver.{AuthTokens, Driver, GraphDatabase}

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created at 11:40 2019/12/11
  * @Modified By:
  */
abstract class PerformanceTest {

  val dir = new File("./itest/performance")
  if (!dir.exists()) {
    dir.mkdirs()
  }

  val outputDir = new File(s"${dir}/output")
  if (!outputDir.exists()) {
    outputDir.mkdirs()
  }

  val gson = new GsonBuilder().enableComplexMapKeySerialization().create()

  val props: Properties = {
    val props = new Properties()
    props.load(new FileInputStream(new File(s"${dir}/performanceConf.properties")))
    props
  }

  def getRecordFile(fileName: String): File = {
    val recordFile = new File(s"${outputDir}/${fileName}")
    if(!recordFile.exists()) {
      recordFile.createNewFile()
    }
    recordFile
  }

  def getStatementsIter(fileName: String): Iterator[String] = {
    val statementFile = new File(s"${dir}/${fileName}")
    val source = Source.fromFile(statementFile, "utf-8")
    val lineIterator = source.getLines()
    lineIterator
  }

  def executeCypher[T <: Driver](cypherList: List[String], driver: T): Array[Long] = {
    val _time0 = System.currentTimeMillis()

    val session = driver.session()
    val _time1 = System.currentTimeMillis()

    val tx = session.beginTransaction()
    val _time2 = System.currentTimeMillis()

    cypherList.foreach(cypher => tx.run(cypher))
    val _time3 = System.currentTimeMillis()

    tx.success()
    tx.close()
    val _time4 = System.currentTimeMillis()

    session.close()
    val _time5 = System.currentTimeMillis()
    Array(_time0, _time1, _time2, _time3, _time4, _time5)
  }

  def fullTest(recordFile: File, recorder: PrintWriter, cmdIter: Iterator[String], driver: Driver): Unit = {
    val cmdArray = cmdIter.toArray
    val _startTime = System.currentTimeMillis()
    val resultLog = new ListBuffer[Future[ResultMap]]

    cmdArray.foreach(cypher => {
      val logItem = Future[ResultMap] {
        val resultMap = new ResultMap(cypher, executeCypher(List(cypher), driver))
        resultMap
      }
      resultLog.append(logItem)
    })

    var _i = 0
    var _successed = 0
    var _failed = 0
    val sum = resultLog.length
    resultLog.foreach(logItem => {
      val resultMap: ResultMap = try {
        _i = _i + 1
        // scalastyle:off
        println(s"Waiting for the ${_i}th of ${sum} result, ${_successed} successed, ${_failed} timeout.")
        val resultMap = Await.result(logItem, 300.seconds)
        _successed += 1
        resultMap
      } catch {
        case timeout: TimeoutException =>
          _failed += 1
          val _timeOutArray = Array(-1.toLong, -1.toLong, -1.toLong, -1.toLong, -1.toLong, -1.toLong)
          val cypher = cmdArray(_i-1)
          new ResultMap(cypher, _timeOutArray)
      }
      val line = gson.toJson(resultMap.getResultMap) + "\n"
      recorder.write(line)
      recorder.flush()
    })
    val _endTime = System.currentTimeMillis()
    recorder.write({s"totalTime:${_endTime - _startTime}"})
    recorder.flush()
  }
}

class Neo4jPerformanceTest extends PerformanceTest {

  val recordFile = getRecordFile(props.getProperty("neo4jResultFile"))
  val recorder = new PrintWriter(recordFile)
  val cmdIter = getStatementsIter(props.getProperty("statementsFile"))

  val driver = GraphDatabase.driver(props.getProperty("boltURI"),
    AuthTokens.basic("neo4j", "bigdata"))

  @Test
  def test1(): Unit = {
    fullTest(recordFile, recorder, cmdIter, driver)
  }

}

class PandaDBPerformanceTest extends PerformanceTest {
  val recordFile = getRecordFile(props.getProperty("PandaDBResultFile"))
  val recorder = new PrintWriter(recordFile)
  val cmdIter = getStatementsIter(props.getProperty("statementsFile"))

  val pandaDriver = GraphDatabase.driver(s"panda://${props.getProperty("zkServerAddr")}/db",
    AuthTokens.basic("", ""))

  @Test
  def test1(): Unit = {
    fullTest(recordFile, recorder, cmdIter, pandaDriver)
  }
}

class MergePerformanceTest extends PandaDBPerformanceTest {

  @Test
  def test0(): Unit = {
    val list = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    list.foreach(i => circularTest(i))
  }

  def circularTest(time: Int): Unit = {
    pandaTest(time)
    neo4jTest(time)
  }

  def pandaTest(time: Int): Unit = {
    val pRecordFile = getRecordFile(s"panda${time}.txt")
    val pRecorder = new PrintWriter(pRecordFile)
    val pCmdIter = getStatementsIter(props.getProperty("statementsFile"))
    val pandaDriver = GraphDatabase.driver(s"panda://${props.getProperty("zkServerAddr")}/db",
      AuthTokens.basic("", ""))
    fullTest(pRecordFile, pRecorder, pCmdIter, pandaDriver)
  }

  def neo4jTest(time: Int): Unit = {
    val nRecordFile = getRecordFile(s"neo4j${time}.txt")
    val nRecorder = new PrintWriter(nRecordFile)
    val nCmdIter = getStatementsIter(props.getProperty("statementsFile"))
    val neo4jDriver = GraphDatabase.driver(props.getProperty("boltURI"),
      AuthTokens.basic("neo4j", "bigdata"))
    fullTest(nRecordFile, nRecorder, nCmdIter, neo4jDriver)
  }
}