package ppd

import java.io.{File, FileInputStream}
import java.util.Properties

import cn.pandadb.server.PNodeServer
import org.junit.Before
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.io.fs.FileUtils
import cn.pandadb.externalprops.{CustomPropertyNodeStore, InSolrPropertyNodeStore}
import cn.pandadb.server.GlobalContext

class InSolrPredicatePushDown extends QueryCase {

  @Before
  def initdb(): Unit = {
    PNodeServer.toString
    new File("./output/testdb").mkdirs();
    FileUtils.deleteRecursively(new File("./output/testdb"));
    db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File("./output/testdb")).
      newGraphDatabase()

    val configFile = new File("./testdata/neo4j.conf")
    val props = new Properties()
    props.load(new FileInputStream(configFile))
    val zkString = props.getProperty("external.properties.store.solr.zk")
    val collectionName = props.getProperty("external.properties.store.solr.collection")
    val solrNodeStore = new InSolrPropertyNodeStore(zkString, collectionName)
    solrNodeStore.clearAll()
    GlobalContext.put(classOf[CustomPropertyNodeStore].getName, solrNodeStore)

  }

}