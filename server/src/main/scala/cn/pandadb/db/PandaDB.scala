package cn.pandadb.db

import java.io.File

import cn.pandadb.context.InstanceBoundServiceFactoryRegistry
import cn.pandadb.cypherplus.SemanticOperatorServiceFactory
import cn.pandadb.driver.CypherService
import cn.pandadb.util.Logging
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.factory.{GraphDatabaseBuilder, GraphDatabaseFactory}
import org.neo4j.kernel.impl.blob.{BlobStorageServiceFactory, DefaultBlobFunctionsServiceFactory}

import scala.collection.JavaConversions

/**
  * Created by bluejoe on 2019/7/17.
  *
  * @deprecated
  */
object PandaDB extends Logging with Touchable {
  InstanceBoundServiceFactoryRegistry.register[BlobStorageServiceFactory];
  InstanceBoundServiceFactoryRegistry.register[DefaultBlobFunctionsServiceFactory];
  InstanceBoundServiceFactoryRegistry.register[SemanticOperatorServiceFactory];

  def openDatabase(dbDir: File, propertiesFile: File, build: (GraphDatabaseBuilder) => Unit): GraphDatabaseService = {
    openDatabase(dbDir, Some(propertiesFile), build)
  }

  def openDatabase(dbDir: File, propertiesFile: File, overrideConfig: Map[String, String] = Map()): GraphDatabaseService = {
    openDatabase(dbDir, Some(propertiesFile), (builder: GraphDatabaseBuilder) => {
      if (!overrideConfig.isEmpty) {
        builder.setConfig(JavaConversions.mapAsJavaMap(overrideConfig))
      }

      {}
    })
  }

  private def openDatabase(dbDir: File, propertiesFileOption: Option[File], build: (GraphDatabaseBuilder) => Unit): GraphDatabaseService = {
    openDatabase(dbDir, (builder: GraphDatabaseBuilder) => {
      if (propertiesFileOption.isDefined) {
        val propertiesFile = propertiesFileOption.get
        logger.info(s"loading configuration from $propertiesFile");
        builder.loadPropertiesFromFile(propertiesFile.getPath);
      }

      build(builder);
    })
  }

  def openDatabase(dbDir: File, build: (GraphDatabaseBuilder) => Unit): GraphDatabaseService = {
    val builder = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbDir);
    build(builder);

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