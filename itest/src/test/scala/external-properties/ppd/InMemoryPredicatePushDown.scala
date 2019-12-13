package ppd

import java.io.File
import cn.pandadb.server.PNodeServer
import org.junit.Before
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.io.fs.FileUtils
import cn.pandadb.externalprops.{CustomPropertyNodeStore, InMemoryPropertyNodeStore, InMemoryPropertyNodeStoreFactory}
import cn.pandadb.server.GlobalContext

class InMemoryPredicatePushDown extends QueryCase {

  @Before
  def initdb(): Unit = {
    PNodeServer.toString
    new File("./output/testdb").mkdirs();
    FileUtils.deleteRecursively(new File("./output/testdb"));
    db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File("./output/testdb")).
      setConfig("external.properties.store.factory", classOf[InMemoryPropertyNodeStoreFactory].getName).
      newGraphDatabase()
    GlobalContext.put(classOf[CustomPropertyNodeStore].getName, InMemoryPropertyNodeStore)
  }

}