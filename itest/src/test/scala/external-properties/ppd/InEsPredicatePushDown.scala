package ppd

import java.io.{File, FileInputStream}
import java.util.Properties

import org.junit.{Before, Test}
import cn.pandadb.externalprops.{ExternalPropertiesContext, InElasticSearchPropertyNodeStore}

class InEsPredicatePushDown extends QueryCase {

  @Before
  def init(): Unit = {
    val configFile = new File("./testdata/neo4j.conf")
    val props = new Properties()
    props.load(new FileInputStream(configFile))

    val esHost = props.getProperty("external.properties.store.es.host")
    val esPort = props.getProperty("external.properties.store.es.port").toInt
    val esSchema = props.getProperty("external.properties.store.es.schema")
    val esIndex = props.getProperty("external.properties.store.es.index")
    val esType = props.getProperty("external.properties.store.es.type")
    val esScrollSize = props.getProperty("external.properties.store.es.scroll.size", "1000").toInt
    val esScrollTime = props.getProperty("external.properties.store.es.scroll.time.minutes", "10").toInt

    val esNodeStore = new InElasticSearchPropertyNodeStore(esHost, esPort, esIndex, esType, esSchema, esScrollSize, esScrollTime)
    ExternalPropertiesContext.bindCustomPropertyNodeStore(esNodeStore)
    esNodeStore.clearAll()
    buildDB(esNodeStore)
  }

}