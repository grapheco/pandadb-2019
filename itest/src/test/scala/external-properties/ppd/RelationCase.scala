package ppd

import java.io.{File, FileInputStream}
import java.util.Properties

import cn.pandadb.externalprops.{ExternalPropertiesContext, InSolrPropertyNodeStore}
import cn.pandadb.util.GlobalContext
import org.junit.Test
import org.neo4j.driver.{AuthTokens, GraphDatabase}

class RelationCase {

  val configFile = new File("./testdata/codeBabyTest.conf")
  val props = new Properties()
  props.load(new FileInputStream(configFile))
  val zkString = props.getProperty("external.properties.store.solr.zk")
  val collectionName = props.getProperty("external.properties.store.solr.collection")

  val solrNodeStore = new InSolrPropertyNodeStore(zkString, collectionName)
  GlobalContext.setLeaderNode(true)

  val uri = "bolt://10.0.82.220:7687"
  val driver = GraphDatabase.driver(uri, AuthTokens.basic("neo4j", "bigdata"))
  val session = driver.session()
  val query = "match (n:person)-[:write_paper]->(p:paper) where p.country = 'United States' AND n.citations>10 return count(n)"

  def run(): Unit = {
    val startTime = System.currentTimeMillis()
    val result = session.run(query)
    val endTime = System.currentTimeMillis()
    println(result.list())
    println(s"query latency: ${endTime-startTime}")
  }

  @Test
  def solr() {
    ExternalPropertiesContext.bindCustomPropertyNodeStore(solrNodeStore)
    run()
  }

  @Test
  def native(): Unit = {
    ExternalPropertiesContext.bindCustomPropertyNodeStore(null)
    run()
  }

}
