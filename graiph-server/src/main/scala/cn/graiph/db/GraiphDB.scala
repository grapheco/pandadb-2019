package cn.graiph.db

import java.io.File

import cn.graiph.context.InstanceBoundServiceFactoryRegistry
import cn.graiph.cypherplus.SemanticOperatorServiceFactory
import cn.graiph.driver.CypherService
import cn.graiph.util.Logging
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.kernel.impl.blob.{DefaultBlobFunctionsServiceFactory, BlobStorageServiceFactory}

/**
  * Created by bluejoe on 2019/7/17.
 *
  * @deprecated
  */
object GraiphDB extends Logging with Touchable {
  InstanceBoundServiceFactoryRegistry.register[BlobStorageServiceFactory];
  InstanceBoundServiceFactoryRegistry.register[DefaultBlobFunctionsServiceFactory];
  InstanceBoundServiceFactoryRegistry.register[SemanticOperatorServiceFactory];

  def openDatabase(dbDir: File, propertiesFile: File): GraphDatabaseService = {
    val builder = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbDir);
    logger.info(s"loading configuration from $propertiesFile");
    builder.loadPropertiesFromFile(propertiesFile.getPath);
    //bolt server is not required
    builder.setConfig("dbms.connector.bolt.enabled", "false");
    builder.newGraphDatabase();
  }

  def connect(dbs: GraphDatabaseService): CypherService = {
    new LocalGraphService(dbs);
  }
}

trait Touchable {
  def touch(): Unit = {
    //do nothing, activate this class static {}
  }
}