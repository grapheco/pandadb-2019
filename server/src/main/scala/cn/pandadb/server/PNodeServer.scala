package cn.pandadb.server

import java.io.File
import java.util.Optional

import cn.pandadb.context.InstanceBoundServiceFactoryRegistry
import cn.pandadb.cypherplus.SemanticOperatorServiceFactory
import cn.pandadb.util.Ctrl._
import cn.pandadb.util.{GlobalContext, Logging}
import org.apache.commons.io.IOUtils
import org.apache.curator.framework.recipes.leader.{LeaderSelector, LeaderSelectorListenerAdapter}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.neo4j.kernel.impl.CustomPropertyNodeStoreHolderFactory
import org.neo4j.kernel.impl.blob.{BlobStorageServiceFactory, DefaultBlobFunctionsServiceFactory}
import org.neo4j.server.{AbstractNeoServer, CommunityBootstrapper}

import scala.collection.JavaConversions

/**
  * Created by bluejoe on 2019/7/17.
  */
object PNodeServer extends Logging {
  val logo = IOUtils.toString(this.getClass.getClassLoader.getResourceAsStream("logo.txt"), "utf-8");
  AbstractNeoServer.NEO4J_IS_STARTING_MESSAGE =
    s"======== PandaDB Node Server(based on Neo4j-3.5.6) ========\r\n${logo}";

  run("registering global database lifecycle service") {
    InstanceBoundServiceFactoryRegistry.register[BlobStorageServiceFactory];
    InstanceBoundServiceFactoryRegistry.register[DefaultBlobFunctionsServiceFactory];
    InstanceBoundServiceFactoryRegistry.register[SemanticOperatorServiceFactory];
    InstanceBoundServiceFactoryRegistry.register[GNodeServerServiceFactory];
    InstanceBoundServiceFactoryRegistry.register[CustomPropertyNodeStoreHolderFactory];
  }

  def startServer(dbDir: File, configFile: File, configOverrides: Map[String, String] = Map()): PNodeServer = {
    val server = new PNodeServer(dbDir, configFile, configOverrides);
    server.start();
    server;
  }
}

class PNodeServer(dbDir: File, configFile: File, configOverrides: Map[String, String] = Map())
  extends LeaderSelectorListenerAdapter {
  val neo4jServer = new CommunityBootstrapper();
  val coordinartor: CoordinartorServer = null;
  val client = CuratorFrameworkFactory.newClient("localhost:2181,localhost:2182,localhost:2183", new ExponentialBackoffRetry(1000, 3));

  def start(): Unit = {
    client.start();
    val leaderSelector = new LeaderSelector(client, "/leader", this);

    leaderSelector.autoRequeue();

    leaderSelector.start();

    neo4jServer.start(dbDir, Optional.of(configFile),
      JavaConversions.mapAsJavaMap(configOverrides));
 }

  def shutdown(): Unit = {
    coordinartor.stop();
    neo4jServer.stop();
  }

  override def takeLeadership(curatorFramework: CuratorFramework): Unit = {
    GlobalContext.put("isLeaderNode", true);
    logger.debug(s"taken leader ship...");
    Thread.sleep(-1);
  }
}

object GNodeServerStarter {
  def main(args: Array[String]) {
    if (args.length != 2) {
      sys.error(s"Usage:\r\n");
      sys.error(s"GNodeServerStarter <db-dir> <conf-file>\r\n");
    }
    else {
      PNodeServer.startServer(new File(args(0)),
        new File(args(1)));
    }
  }
}