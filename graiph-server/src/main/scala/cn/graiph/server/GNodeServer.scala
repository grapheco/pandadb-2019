package cn.graiph.server

import java.io.File
import java.util.Optional
import cn.graiph.cypherplus.SemanticOperatorServiceFactory
import cn.graiph.context.InstanceBoundServiceFactoryRegistry
import cn.graiph.db.Touchable
import cn.graiph.util.{Ctrl, Logging}
import org.apache.commons.io.IOUtils
import org.neo4j.kernel.impl.CustomPropertyNodeStoreHolderFactory
import org.neo4j.kernel.impl.blob.{BlobStorageServiceFactory, DefaultBlobFunctionsServiceFactory}
import org.neo4j.server.{AbstractNeoServer, CommunityBootstrapper}
import Ctrl._
import scala.collection.JavaConversions

/**
  * Created by bluejoe on 2019/7/17.
  */
object GNodeServer extends Logging with Touchable {
  val logo = IOUtils.toString(this.getClass.getClassLoader.getResourceAsStream("pandaLogo1.txt"), "utf-8");
  AbstractNeoServer.NEO4J_IS_STARTING_MESSAGE = "======== Graiph Node Server(based on Neo4j-3.5.6) ======== " + "\r\n" + logo;

  run("registering global database lifecycle service") {
    InstanceBoundServiceFactoryRegistry.register[BlobStorageServiceFactory];
    InstanceBoundServiceFactoryRegistry.register[DefaultBlobFunctionsServiceFactory];
    InstanceBoundServiceFactoryRegistry.register[SemanticOperatorServiceFactory];
    InstanceBoundServiceFactoryRegistry.register[GNodeServerServiceFactory];
    InstanceBoundServiceFactoryRegistry.register[CustomPropertyNodeStoreHolderFactory];
  }

  def startServer(dbDir: File, configFile: File, configOverrides: Map[String, String] = Map()): GNodeServer = {
    val server = new GNodeServer(dbDir, configFile, configOverrides);
    server.start();
    server;
  }
}

class GNodeServer(dbDir: File, configFile: File, configOverrides: Map[String, String] = Map()) {
  val neo4jServer = new CommunityBootstrapper();
  val coordinartor: CoordinartorServer = null;

  def start(): Int = {
    neo4jServer.start(dbDir, Optional.of(configFile),
      JavaConversions.mapAsJavaMap(configOverrides));
  }

  def shutdown(): Int = {
    coordinartor.stop();
    neo4jServer.stop();
  }
}

object GNodeServerStarter {
  def main(args: Array[String]) {
    if (args.length != 2) {
      sys.error(s"Usage:\r\n");
      sys.error(s"GNodeServerStarter <db-dir> <conf-file>\r\n");
    }
    else {
      GNodeServer.startServer(new File(args(0)),
        new File(args(1)));
    }
  }
}