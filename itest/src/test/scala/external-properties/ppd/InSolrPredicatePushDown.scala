package ppd

import java.io.{File, FileInputStream}
import java.util.Properties

import org.junit.Before
import cn.pandadb.externalprops.{CustomPropertyNodeStore, InSolrPropertyNodeStore}
import cn.pandadb.server.GlobalContext

class InSolrPredicatePushDown extends QueryCase {

  @Before
  def init(): Unit = {
    val configFile = new File("./testdata/neo4j.conf")
    val props = new Properties()
    props.load(new FileInputStream(configFile))
    val zkString = props.getProperty("external.properties.store.solr.zk")
    val collectionName = props.getProperty("external.properties.store.solr.collection")
    val solrNodeStore = new InSolrPropertyNodeStore(zkString, collectionName)
    GlobalContext.put(classOf[CustomPropertyNodeStore].getName, solrNodeStore)
    solrNodeStore.clearAll()
    buildDB(solrNodeStore)
  }

}