package cn.pandadb.server

import java.io.File
import java.util.Optional
import java.util.concurrent.CountDownLatch

import cn.pandadb.context.InstanceBoundServiceFactoryRegistry
import cn.pandadb.cypherplus.SemanticOperatorServiceFactory
import cn.pandadb.network.ClusterClient
import cn.pandadb.util.Ctrl._
import cn.pandadb.util.{ContextMap, Logging}
import org.apache.commons.io.IOUtils
import org.apache.curator.framework.recipes.leader.{LeaderSelector, LeaderSelectorListenerAdapter}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.neo4j.kernel.impl.CustomPropertyNodeStoreHolderFactory
import org.neo4j.kernel.impl.blob.{BlobStorageServiceFactory, DefaultBlobFunctionsServiceFactory}
import org.neo4j.server.CommunityBootstrapper

import scala.collection.JavaConversions

/**
  * Created by bluejoe on 2019/7/17.
  */
object PNodeServer extends Logging {
  val logo = IOUtils.toString(this.getClass.getClassLoader.getResourceAsStream("logo.txt"), "utf-8");

  run("registering global database lifecycle service") {
    InstanceBoundServiceFactoryRegistry.register[BlobStorageServiceFactory];
    InstanceBoundServiceFactoryRegistry.register[DefaultBlobFunctionsServiceFactory];
    InstanceBoundServiceFactoryRegistry.register[SemanticOperatorServiceFactory];
    InstanceBoundServiceFactoryRegistry.register[CustomPropertyNodeStoreHolderFactory];
  }

  def startServer(dbDir: File, configFile: File, configOverrides: Map[String, String] = Map()): PNodeServer = {
    val server = new PNodeServer(dbDir, configFile, configOverrides);
    server.start();
    server;
  }
}

object PNodeServerContext extends ContextMap {

  def bindClusterClient(client: ClusterClient): Unit =
    this.put[ClusterClient](client)

  def getClusterClient: ClusterClient = this.get[ClusterClient]

  def bindLeaderNode(boolean: Boolean): Unit =
    this.put("is.leader.node", boolean)

  def isLeaderNode: Boolean = this.getOption("is.leader.node").getOrElse(false)
}

// This class need to be modified, replace hard code.
class PNodeServer(dbDir: File, configFile: File, configOverrides: Map[String, String] = Map())
  extends LeaderSelectorListenerAdapter with Logging {
  //TODO: we will replace neo4jServer with InterNodeRpcServer someday!!
  val neo4jServer = new CommunityBootstrapper();
  val coordinator: CoordinatorServer = null;
  val client = CuratorFrameworkFactory.newClient("localhost:2181,localhost:2182,localhost:2183", new ExponentialBackoffRetry(1000, 3));
  val runningLock = new CountDownLatch(1)

  def start(): Unit = {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      override def run(): Unit = {
        shutdown();
      }
    });

    val clusterClient: ClusterClient = null;

    PNodeServerContext.bindClusterClient(clusterClient);
    client.start();
    val leaderSelector = new LeaderSelector(client, "/panda/leader", this);
    //leaderSelector.autoRequeue();
    leaderSelector.start();

    neo4jServer.start(dbDir, Optional.of(configFile),
      JavaConversions.mapAsJavaMap(configOverrides));
  }

  def shutdown(): Unit = {
    /*
    if (neo4jServer.isRunning) {
      neo4jServer.stop();
    }
    */

    runningLock.countDown()
    coordinator.stop();
  }

  override def takeLeadership(curatorFramework: CuratorFramework): Unit = {
    PNodeServerContext.bindLeaderNode(true);
    logger.debug(s"taken leader ship...");
    //yes, i won't quit, never!
    runningLock.await()
    logger.debug(s"shutdown...");
  }
}