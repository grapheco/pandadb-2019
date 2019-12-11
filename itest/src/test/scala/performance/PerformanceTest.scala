package performance

import java.io.{File, FileInputStream, PrintWriter}
import java.util.Properties

import com.google.gson.GsonBuilder
import org.junit.Test
import org.neo4j.driver.{AuthTokens, Driver, GraphDatabase}

import scala.io.Source



/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created at 11:40 2019/12/11
  * @Modified By:
  */
trait PerformanceTest {

  val dir = new File("./itest/performance")
  if (!dir.exists()) {
    dir.mkdirs()
  }

  val gson = new GsonBuilder().enableComplexMapKeySerialization().create()

  val props: Properties = {
    val props = new Properties()
    props.load(new FileInputStream(new File(s"${dir}/performanceConf.properties")))
    props
  }

  def getRecordFile(fileName: String): File = {
    val recordFile = new File(s"${dir}/${fileName}")
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

  def executeCypher[T <: Driver](cypher: String, driver: T): Array[Long] = {

    val _time0 = System.currentTimeMillis()

    val session = driver.session()
    val _time1 = System.currentTimeMillis()

    val tx = session.beginTransaction()
    val _time2 = System.currentTimeMillis()

    tx.run(cypher)
    val _time3 = System.currentTimeMillis()

    tx.success()
    tx.close()
    val _time4 = System.currentTimeMillis()

    session.close()
    val _time5 = System.currentTimeMillis()

    Array(_time0, _time1, _time2, _time3, _time4, _time5)
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
    cmdIter.toList.foreach(cypher => {
      recorder.write(
        gson.toJson(
          new ResultMap(cypher,
            executeCypher(cypher, driver)).getResultMap) + "\n")
    })
    recorder.flush()
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
    cmdIter.toList.foreach(cypher => {
      recorder.write(
        gson.toJson(
          new ResultMap(cypher,
            executeCypher(cypher, pandaDriver)).getResultMap) + "\n")
    })
    recorder.flush()
  }
}

