package cn.pandadb.server

import java.util.ServiceLoader
import java.io.File

import cn.pandadb.blob.storage.impl.RegionfsBlobValueStorage

import scala.collection.JavaConverters._
import cn.pandadb.configuration.Config
import cn.pandadb.lifecycle.LifecycleSupport
import cn.pandadb.cluster.ClusterService
import cn.pandadb.datanode.PandaRpcServer
import cn.pandadb.index.IndexServiceFactory
import cn.pandadb.server.Store.{DataStore, DataStoreLayout}
import cn.pandadb.zk.ZKTools

class PandaServer(config: Config)  {

  val life = new LifecycleSupport
  val logger = config.getLogger(this.getClass)

//  val localNeo4jDB = getOrCreateLocalNeo4jDatabase()
  val clusterService = new ClusterService(config)
  clusterService.init()

  val localStorePath = config.getLocalDataStorePath()
  val localDataStore: DataStore = new DataStore(DataStoreLayout.of(new File(localStorePath)))
  val clusterNodeServer = new ClusterNodeServer(config, clusterService, localDataStore)
  life.add(clusterNodeServer)

  val serviceLoaders = ServiceLoader.load(classOf[IndexServiceFactory]).asScala
  if(serviceLoaders.size > 0) {
    val indexService = serviceLoaders.iterator.next().create(config)
    life.add(indexService )
  }

  val blobStoreService = new RegionfsBlobValueStorage(config)
  life.add(blobStoreService)

  life.add(new PandaRpcServer(config, clusterService, blobStoreService, localDataStore) )
  clusterService.start()

  def start(): Unit = {
    logger.info("==== PandaDB Server Starting... ====")
    life.start()
    logger.info("==== PandaDB Server is Started ====")
  }

  def shutdown(): Unit = {
    logger.info("==== PandaDB Server Shutting Down... ====")
    life.shutdown()
    logger.info("==== PandaDB Server is Shutdown ====")
  }

  def syncDataFromLeader(): Unit = {
    logger.info(this.getClass + ": syncDataFromLeader")
    val leaderNodeAddress = clusterService.getLeaderNode()
    val localDBPath = config.getLocalNeo4jDatabasePath()
    logger.info(this.getClass + s"sync data from leaderNode<$leaderNodeAddress> to local<$localDBPath>")
  }



//  def getOrCreateLocalNeo4jDatabase(): GraphDatabaseService = {
//    val dbFile = new File(config.getLocalNeo4jDatabasePath())
//    if (!dbFile.exists()) {
//      dbFile.mkdirs
//    }
//    new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbFile).newGraphDatabase()
//  }
}
