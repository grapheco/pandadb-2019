package ppd

import java.io.{File, FileInputStream}
import java.util.Properties

import org.junit.Before
import cn.pandadb.externalprops.{ExternalPropertiesContext, CustomPropertyNodeStore, InSolrPropertyNodeStore}

class InSolrPredicatePushDown extends QueryCase {

  @Before
  def init(): Unit = {
    val configFile = new File("./testdata/neo4j.conf")
    val props = new Properties()
    props.load(new FileInputStream(configFile))
    val zkString = props.getProperty("external.properties.store.solr.zk")
    val collectionName = props.getProperty("external.properties.store.solr.collection")
    val solrNodeStore = new InSolrPropertyNodeStore(zkString, collectionName)
    ExternalPropertiesContext.bindCustomPropertyNodeStore(solrNodeStore)
    solrNodeStore.clearAll()
    buildDB(solrNodeStore)
  }

}