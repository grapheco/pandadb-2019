package cn.graiph.server

import java.io.File
import java.util.Optional

import cn.graiph.db.{GraiphDB, Touchable}
import org.apache.commons.io.IOUtils
import org.neo4j.blob.utils.Logging
import org.neo4j.server.{AbstractNeoServer, CommunityBootstrapper}

import scala.collection.JavaConversions

/**
  * Created by bluejoe on 2019/7/17.
  */
object GraiphServer extends Logging with Touchable {
  val logo = IOUtils.toString(this.getClass.getClassLoader.getResourceAsStream("logo.txt"), "utf-8");
  AbstractNeoServer.NEO4J_IS_STARTING_MESSAGE = "======== Graiph (Neo4j-3.5.6-BLOB) ======== "+"\r\n"+logo;
  GraiphDB.touch;

  def startServer(dbDir: File, configFile: File, configOverrides: Map[String, String] = Map()): GraiphServer = {
    val server = new GraiphServer(dbDir, configFile, configOverrides);
    server.start();
    server;
  }
}

class GraiphServer(dbDir: File, configFile: File, configOverrides: Map[String, String] = Map()) {
  val server = new CommunityBootstrapper();

  def start(): Int = {
    server.start(dbDir, Optional.of(configFile),
      JavaConversions.mapAsJavaMap(configOverrides));
  }

  def shutdown(): Int = {
    server.stop();
  }
}

object GraiphServerStarter {
  def main(args: Array[String]) {
    if (args.length != 2) {
      sys.error(s"Usage:\r\n");
      sys.error(s"\tGraiphServerStarter <db-dir> <conf-file>\r\n");
    }
    else {
      GraiphServer.startServer(new File(args(0)),
        new File(args(1)));
    }
  }
}