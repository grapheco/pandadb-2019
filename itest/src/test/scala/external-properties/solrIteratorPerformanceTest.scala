package external

import java.io.{File, FileInputStream}
import java.util.Properties

import cn.pandadb.externalprops.{InSolrPropertyNodeStore, SolrQueryResults}
import org.apache.solr.client.solrj.SolrQuery
import org.junit.{Assert, Test}

class solrIteratorPerformanceTest {

  val configFile = new File("./testdata/codeBabyTest.conf")
  val props = new Properties()
  props.load(new FileInputStream(configFile))
  val zkString = props.getProperty("external.properties.store.solr.zk")
  val collectionName = props.getProperty("external.properties.store.solr.collection")

  //scalastyle:off
  @Test
  def test1() {
    println(zkString)
    println(collectionName)

    val solrNodeStore = new InSolrPropertyNodeStore(zkString, collectionName)

    val size = solrNodeStore.getRecorderSize
    Assert.assertEquals(13738580, size)

    println(size)
    var _startTime = System.currentTimeMillis()
    //val query = new SolrQuery("country:Finland")
    val query = new SolrQuery("country:Finland")
    val res = new SolrQueryResults(solrNodeStore._solrClient, query, 10000)
    val it = res.iterator2().toIterable
   // it.foreach(u => println(u.id))
    println(it.size)
    //it.getCurrentData().foreach(u => println(u))
  /*  println(it.getCurrentData().size)
    _startTime = System.currentTimeMillis()
    var i = 0
    while (it.readNextPage()) {
      i += 1
      val _endTime = System.currentTimeMillis()
      println({s"$i--Time:${_endTime - _startTime}"})
      //it.getCurrentData().foreach(u => println(u))
      println(it.getCurrentData().size)
    }
    val _endTime = System.currentTimeMillis()

    println({s"iterator-totalTime:${_endTime - _startTime}"})

   // res.getAllResults()

    val _endTime1 = System.currentTimeMillis()

    println({s"getall-totalTime:${_endTime1 - _endTime}"})
*/

  }

}
