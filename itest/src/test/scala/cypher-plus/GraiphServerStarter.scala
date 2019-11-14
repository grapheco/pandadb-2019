import java.io.File
import cn.graiph.server.GNodeServer

/**
  * Created by bluejoe on 2019/7/17.
  */
object GraiphServerStarter {
  def main(args: Array[String]) {
    GNodeServer.startServer(new File("./output/testdb"),
      new File("./testdata/neo4j.conf"));
  }
}
