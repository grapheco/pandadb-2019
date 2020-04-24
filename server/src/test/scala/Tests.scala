
import java.io.File
import java.util.Optional

import org.junit.Test

import scala.collection.JavaConverters._
import org.neo4j.server.CommunityBootstrapper

class Tests {
  val neo4jServer = new CommunityBootstrapper()
  val dbFile = new File("./output/testdb")

  @Test
  def test(): Unit = {
    neo4jServer.start(dbFile, Optional.empty(), Map[String, String]().asJava);
  }

}
