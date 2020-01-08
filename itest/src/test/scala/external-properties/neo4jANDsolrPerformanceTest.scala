package externals

import java.io.{File, FileInputStream}
import java.util.Properties

import cn.pandadb.externalprops.InSolrPropertyNodeStore
import org.apache.solr.client.solrj.SolrQuery
import org.junit.{Assert, Test}
import org.neo4j.cypher.internal.runtime.interpreted.{NFAnd, NFEquals, NFGreaterThan, NFLessThan}
import org.neo4j.driver.{AuthTokens, GraphDatabase}
import org.neo4j.values.storable.Values
import org.scalatest.selenium.WebBrowser.Query

class neo4jANDsolrPerformanceTest {

  val configFile = new File("./testdata/codeBabyTest.conf")
  val props = new Properties()
  props.load(new FileInputStream(configFile))
  val zkString = props.getProperty("external.properties.store.solr.zk")
  val collectionName = props.getProperty("external.properties.store.solr.collection")

  val solrNodeStore = new InSolrPropertyNodeStore(zkString, collectionName)

  //val size = solrNodeStore.getRecorderSize
  //Assert.assertEquals(13738580, size)
  val uri = "bolt://10.0.82.220:7687"
  val driver = GraphDatabase.driver(uri,
    AuthTokens.basic("neo4j", "bigdata"))
  //val res = driver.session().run("match (n) return count(n)")
  //scalastyle:off
  val session = driver.session()
  @Test
  def test1() {

   // println(solrNodeStore.getRecorderSize)
   // println(res.next())
    val n1 = "match (n) where id(n)=8853096 return n"
    val s1 = NFEquals("id", Values.of(8853096))

    val n2 = "match (n:organization) where n.citations>985 return n"
    val s21 = NFGreaterThan("citations", Values.of(985))
    val s22 = NFEquals("labels", Values.of("organization"))
    val s2 = NFAnd(s21 , s22)

    val n3 = "match (n) where n.nationality='France' return n"
    val s3 = NFEquals("nationality", Values.of("France"))

    val n4 = "match (n:organization) return n"
    val s4 = NFEquals("labels", Values.of("organization"))

    val n5 = "match (n:person) where n.citations>100 and n.citations5<200 and n.nationality='Russia' return n"
    val s51 = NFEquals("labels", Values.of("person"))
    val s52 = NFGreaterThan("citations", Values.of(100))
    val s53 = NFLessThan("citations5", Values.of(200))
    val s54 = NFEquals("nationality", Values.of("Russia"))
    val s512 = NFAnd(s51, s52)
    val s534 = NFAnd(s53, s54)
    val s5 = NFAnd(s512, s534)

    val n6 = "match (n) where n.citations>100 and n.citations<150 return n"
    val s61 = NFGreaterThan("citations", Values.of(100))
    val s62 = NFLessThan("citations", Values.of(150))
    val s6 = NFAnd(s61, s62)


    //citations:
    //281
    //citations5:
    //210
    //nameEn:
    //Dmitry Mansfeld
    //nationality:
    //Russia
    val nquery = n6
    val query = s6

    val time1 = System.currentTimeMillis()
    val ssize = solrNodeStore.filterNodes(query)
    val time2 = System.currentTimeMillis()
    val nsize = session.run(nquery)
    val time3 = System.currentTimeMillis()

    println(s"solr query time :${time2-time1},query size:$ssize")
    println(s"neo4j query time :${time3-time2},query size:$nsize")

  }

}
