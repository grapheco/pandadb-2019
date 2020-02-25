package externals

import java.io.{File, FileInputStream}
import java.util.Properties
import java.util.function.Consumer

import cn.pandadb.externalprops.{InSolrPropertyNodeStore, NodeWithProperties, SolrQueryResults, SolrUtil}
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.junit.{Assert, Test}
import org.neo4j.cypher.internal.runtime.interpreted.{NFAnd, NFEquals, NFGreaterThan, NFLessThan, NFPredicate}
import org.neo4j.driver.{AuthTokens, GraphDatabase}
import org.neo4j.values.storable.Values
import org.scalatest.selenium.WebBrowser.Query

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

class neo4jANDsolrPerformanceTest {

  val configFile = new File("./testdata/codeBabyTest.conf")
  val props = new Properties()
  props.load(new FileInputStream(configFile))
  val zkString = props.getProperty("external.properties.store.solr.zk")
  val collectionName = props.getProperty("external.properties.store.solr.collection")

  val solrNodeStore = new InSolrPropertyNodeStore(zkString, collectionName)
  val _solrClient = {
    val client = new CloudSolrClient(zkString);
    client.setZkClientTimeout(30000);
    client.setZkConnectTimeout(50000);
    client.setDefaultCollection(collectionName);
    client
  }

  //val size = solrNodeStore.getRecorderSize
  //Assert.assertEquals(13738580, size)
  val uri = "bolt://10.0.82.220:7687"
  val driver = GraphDatabase.driver(uri,
    AuthTokens.basic("neo4j", "bigdata"))
  //val res = driver.session().run("match (n) return count(n)")
  //scalastyle:off
  val session = driver.session()

  def solrIteratorTime(exp: NFPredicate): Unit ={
    val time1 = System.currentTimeMillis()
    val ssize = solrNodeStore.filterNodesWithProperties(exp).size
    val time2 = System.currentTimeMillis()
    println(s"solr Iterator time :${time2-time1},result size:$ssize")
  }

  def neo4jTime(cypher: String): Unit ={
    val time1 = System.currentTimeMillis()
    val nsize = session.run(cypher).list().size()
    val time2 = System.currentTimeMillis()
    println(s"neo4j Iterator time :${time2-time1},result size:$nsize")
  }

  def solrArray(q: String): Unit ={
    val nodeArray = ArrayBuffer[NodeWithProperties]()
    val solrQuery = new SolrQuery()
    solrQuery.set(q)
    val time1 = System.currentTimeMillis()
    val size = _solrClient.query(solrQuery).getResults.getNumFound
    solrQuery.setRows(size.toInt)

    val res = _solrClient.query(solrQuery).getResults
    res.foreach(u => nodeArray += SolrUtil.solrDoc2nodeWithProperties(u))
    val ssize = nodeArray.size
    val time2 = System.currentTimeMillis()
    println(s"solr Array time :${time2-time1},result size:$ssize")

  }

  def test(exp: NFPredicate = null, cypher: String = null, q: String = null): Unit ={

    if(q!=null) {
      solrArray(q)
    }

    if (cypher!=null) {
      neo4jTime(cypher)
    }

    if (exp!=null) {
      solrIteratorTime(exp)
    }

  }

  def testFiveFilters(): Unit ={

    val s71 = NFEquals("labels", Values.of("person"))
    val s72 = NFGreaterThan("citations", Values.of(400))
    val s73 = NFGreaterThan("citations5", Values.of(80))
    val s74 = NFLessThan("citations", Values.of(450))
    val s75 = NFLessThan("citations5", Values.of(100))
    val s76 = NFEquals("nationality", Values.of("China"))
    val s712 = NFAnd(s71, s72)
    val s734 = NFAnd(s73, s74)
    val s756 = NFAnd(s75, s76)
    val s71234 = NFAnd(s712, s734)
    val s7 = NFAnd(s71234, s756)
    val n7 = "MATCH (n:person) where n.citations>400 and n.citations<450 and n.nationality='China' and n.citations5<100  and n.citations5>80 return n"

    val s81 = NFEquals("labels", Values.of("organization"))
    val s82 = NFEquals("country", Values.of("China"))
    val s83 = NFGreaterThan("citations", Values.of(200000))
    val s84 = NFGreaterThan("citations5", Values.of(140000))
    val s85 = NFLessThan("citations", Values.of(500000))
    val s812 = NFAnd(s81, s82)
    val s834 = NFAnd(s83, s84)
    val s81234 = NFAnd(s812, s834)
    val s8 = NFAnd(s81234, s85)
    val n8 = "MATCH (n:organization) where n.citations>200000 and n.citations<500000 and n.country='China' and n.citations5>140000  RETURN n"

    solrIteratorTime(s7)
    neo4jTime(n7)

    solrIteratorTime(s8)
    neo4jTime(n8)

  }

  def testThreeFilter(): Unit ={

    val sq4501 = NFEquals("labels", Values.of("paper"))
    val sq4502 = NFGreaterThan("citation", Values.of(350))
    val sq4503 = NFEquals("country", Values.of("India"))
    val s4q5012 = NFAnd(sq4501, sq4502)
    val s4q50123 = NFAnd(s4q5012, sq4503)
    val n4q50 = "MATCH (n:paper) where n.citation >350 and n.country='India' return n"

    val sq5501 = NFEquals("labels", Values.of("person"))
    val sq5502 = NFGreaterThan("citations", Values.of(100000))
    val sq5503 = NFEquals("nationality", Values.of("China"))
    val s5q5012 = NFAnd(sq5501, sq5502)
    val s5q50123 = NFAnd(s5q5012, sq5503)
    val n5q50 = "MATCH (n:person) where n.citations>100000 and n.nationality='China' return n"


    val sq6501 = NFEquals("labels", Values.of("organization"))
    val sq6502 = NFGreaterThan("citations", Values.of(2000000))
    val sq6503 = NFEquals("country", Values.of("China"))
    val s6q5012 = NFAnd(sq6501, sq6502)
    val s6q50123 = NFAnd(s6q5012, sq6503)
    val n6q50 = "MATCH (n:organization) where n.citations>2000000 and n.country='China'  RETURN n"

    solrIteratorTime(s4q50123)
   neo4jTime(n4q50)
   solrIteratorTime(s5q50123)
   neo4jTime(n5q50)
   solrIteratorTime(s6q50123)
   neo4jTime(n6q50)

  }

  def testLessThan50(): Unit ={

    val sq501 = NFEquals("labels", Values.of("organization"))
    val sq502 = NFGreaterThan("citations", Values.of(60000000))
    val sq5012 = NFAnd(sq501, sq502)
    val s1q50 = sq5012
    val n1q50 = "match (n:organization) where n.citations>60000000 return n"

    val sq2501 = NFEquals("labels", Values.of("paper"))
    val sq2502 = NFGreaterThan("citation", Values.of(1500))
    val s2q50 = NFAnd(sq2501, sq2502)
    val n2q50 = "MATCH (n:paper) where n.citation >1500 return n"

    val sq3501 = NFEquals("labels", Values.of("person"))
    val sq3502 = NFGreaterThan("citations", Values.of(400000))
    val s3q50 = NFAnd(sq3501, sq3502)
    val n3q50 = "MATCH (n:person) where n.citations>400000 return n"


    solrIteratorTime(s1q50)
    neo4jTime(n1q50)
    solrIteratorTime(s2q50)
    neo4jTime(n2q50)
    solrIteratorTime(s3q50)
    neo4jTime(n3q50)

  }

  def testForeach(): Unit ={

    val n1 = "match (n) where id(n)=8853096 return n"
    val s1 = NFEquals("id", Values.of(8853096))
    val ss1 = "id:8853096"


    val n2 = "match (n:organization) where n.citations>985 return n"
    val s21 = NFGreaterThan("citations", Values.of(985))
    val s22 = NFEquals("labels", Values.of("organization"))
    val s2 = NFAnd(s21 , s22)
    val ss2 = "labels:organization && citations:{ 985 TO *}"

    val n3 = "match (n) where n.nationality='France' return n"
    val s3 = NFEquals("nationality", Values.of("France"))
    val ss3 = "nationality:France"

    val n4 = "match (n:organization) return n"
    val s4 = NFEquals("labels", Values.of("organization"))
    val ss4 = "labels:organization"

    val n5 = "match (n:person) where n.citations>100 and n.citations5<200 and n.nationality='Russia' return n"
    val s51 = NFEquals("labels", Values.of("person"))
    val s52 = NFGreaterThan("citations", Values.of(100))
    val s53 = NFLessThan("citations5", Values.of(200))
    val s54 = NFEquals("nationality", Values.of("Russia"))
    val s512 = NFAnd(s51, s52)
    val s534 = NFAnd(s53, s54)
    val s5 = NFAnd(s512, s534)
    val ss5 = "(labels:person && citations:{ 100 TO * }) && (nationality:Russia && citations5:{ * TO 200 })"

    val n6 = "match (n) where n.citations>100 and n.citations<150 return n"
    val s61 = NFGreaterThan("citations", Values.of(100))
    val s62 = NFLessThan("citations", Values.of(150))
    val s6 = NFAnd(s61, s62)
    val ss6 = "citations:{ 100 TO 150 }"

    test(s1, n1, ss1)
    test(s2, n2, ss2)
    test(s3, n3, ss3)
    test(s4, n4, ss4)
    test(s5, n5, ss5)
    test(s6, n6, ss6)



  }

  @Test
  def test1() {


  //  testForeach()
   // testFiveFilters()
   // testLessThan50()
  //  testThreeFilter()


  }

}

