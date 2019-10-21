import java.io.File
import cn.graiph.server.GraiphServer

/**
  * Created by bluejoe on 2019/7/17.
  */
object GraiphServerStarter {
  def main(args: Array[String]) {
    GraiphServer.startServer(new File("./testdata/testdb"),
      new File("./testdata/neo4j.conf"));
  }
}
