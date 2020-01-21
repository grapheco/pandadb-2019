package perfomance

import java.io.{File, FileInputStream, FileWriter}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import cn.pandadb.externalprops.{ExternalPropertiesContext, InElasticSearchPropertyNodeStore}
import cn.pandadb.util.GlobalContext
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.factory.GraphDatabaseFactory

import scala.io.Source


trait TestBase {

  def nowDate: String = {
    val now = new Date
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    dateFormat.format(now)
  }

}


object PerformanceTests extends TestBase {

  var esNodeStores: Option[InElasticSearchPropertyNodeStore] = None

  def createPandaDB(props: Properties): GraphDatabaseService = {
    var graphPath = ""
    if (props.containsKey("graph.data.path")) graphPath = props.get("graph.data.path").toString
    else throw new Exception("Configure File Error: graph.data.path is not exist! ")
    val graphFile = new File(graphPath)
    if (!graphFile.exists) throw new Exception(String.format("Error: GraphPath(%s) is not exist! ", graphPath))

    val esHost = props.getProperty("external.properties.store.es.host")
    val esPort = props.getProperty("external.properties.store.es.port").toInt
    val esSchema = props.getProperty("external.properties.store.es.schema")
    val esIndex = props.getProperty("external.properties.store.es.index")
    val esType = props.getProperty("external.properties.store.es.type")
    val esScrollSize = props.getProperty("external.properties.store.es.scroll.size", "1000").toInt
    val esScrollTime = props.getProperty("external.properties.store.es.scroll.time.minutes", "10").toInt
    val esNodeStore = new InElasticSearchPropertyNodeStore(esHost, esPort, esIndex, esType, esSchema, esScrollSize, esScrollTime)
    ExternalPropertiesContext.bindCustomPropertyNodeStore(esNodeStore)
    GlobalContext.setLeaderNode(true)
    esNodeStores = Some(esNodeStore)

    new GraphDatabaseFactory().newEmbeddedDatabase(graphFile)
  }

  def createNeo4jDB(props: Properties): GraphDatabaseService = {
    var graphPath = ""
    if (props.containsKey("graph.data.path")) graphPath = props.get("graph.data.path").toString
    else throw new Exception("Configure File Error: graph.data.path is not exist! ")
    val graphFile = new File(graphPath)
    if (!graphFile.exists) throw new Exception(String.format("Error: GraphPath(%s) is not exist! ", graphPath))

    new GraphDatabaseFactory().newEmbeddedDatabase(graphFile)
  }

  def main(args: Array[String]): Unit = {

    var propFilePath = "/home/bigdata/pandadb-2019/itest/testdata/performance-test.conf" // null;
    if (args.length > 0) propFilePath = args(0)
    val props = new Properties
    props.load(new FileInputStream(new File(propFilePath)))

    var testDb = "neo4j"
    var graphPath = ""
    var logFileDir = ""
    var cyhperFilePath = ""

    if (props.containsKey("test.db")) testDb = props.get("test.db").toString.toLowerCase()
    else throw new Exception("Configure File Error: test.db is not exist! ")

    if (props.containsKey("graph.data.path")) graphPath = props.get("graph.data.path").toString
    else throw new Exception("Configure File Error: graph.data.path is not exist! ")

    if (props.containsKey("log.file.dir")) logFileDir = props.get("log.file.dir").toString
    else throw new Exception("Configure File Error: log.file.dir is not exist! ")

    if (props.containsKey("test.cyhper.path")) cyhperFilePath = props.get("test.cyhper.path").toString
    else throw new Exception("Configure File Error: test.cyhper.path is not exist! ")

    val logDir: File = new File(logFileDir)
    if (!logDir.exists()) {
      logDir.mkdirs
      println("make log dir")
    }
    val logFileName = new SimpleDateFormat("MMdd-HHmmss").format(new Date) + ".log"
    val logFile = new File(logDir, logFileName)

    println("Neo4j Test")
    println(s"GraphDataPath: ${graphPath} \n LogFilePath: ${logFile.getAbsolutePath}")

    val logFw = new FileWriter(logFile)
    logFw.write(s"GraphDataPath: $graphPath \n")
    val cyhpers = readCyphers(cyhperFilePath)
    var db: GraphDatabaseService = null

    if (testDb.equals("neo4j")) {
      println(s"testDB: neo4j \n")
      logFw.write(s"testDB: neo4j \n")
      db = createNeo4jDB(props)
    }
    else if (testDb.equals("pandadb")) {
      println(s"testDB: pandadb \n")
      logFw.write(s"testDB: pandadb \n")
      db = createPandaDB(props)
    }

    if (db == null) {
      throw new Exception("DB is null")
    }

    println("==== begin tests ====")
    val beginTime = nowDate
    println(beginTime)

    try {
      var i = 0
      cyhpers.foreach(cyhper => {
        i += 1
        val tx = db.beginTx()
        val mills0 = System.currentTimeMillis()
        val res = db.execute(cyhper)
        val useMills = System.currentTimeMillis() - mills0
        tx.close()
        println(s"$i, $useMills")
        logFw.write(s"\n====\n$cyhper\n")
        logFw.write(s"UsedTime(ms): $useMills \n")
        logFw.flush()
      })
    }
    finally {
      logFw.close()
      if (testDb == "pandadb" && esNodeStores.isDefined) {
        esNodeStores.get.esClient.close()
      }
      db.shutdown()
    }

    println("==== end tests ====")
    val endTime = nowDate
    println("Begin Time: " + beginTime)
    println("End Time: " + endTime)

  }

  def readCyphers(filePath: String): Iterable[String] = {
    val source = Source.fromFile(filePath, "UTF-8")
    val lines = source.getLines().toArray
    source.close()
    lines
  }

}
