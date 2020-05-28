package cn.pandadb.server.Store

import java.io.File

object StoreFileNames {
  val LOCAL_DATA_VERSION = "local.data.version"
  val LOCAL_DATA_LOG = "local.data.log"
  val DATABASE_NAME = "graph.db"
}

class DataStoreLayout(storeDirectory: File) {

  def getDatabaseName: String = StoreFileNames.DATABASE_NAME

  def databaseDirectory: File = file(StoreFileNames.DATABASE_NAME)

  def localDataVersionStore: File = file(StoreFileNames.LOCAL_DATA_VERSION)

  def localDataLogStore: File = file(StoreFileNames.LOCAL_DATA_LOG)

  private def file(fileName: String): File = new File(storeDirectory, fileName)
}

object DataStoreLayout {
  def of(dataStoreDirectory: File): DataStoreLayout = new DataStoreLayout(dataStoreDirectory)

  def layout(): Unit = {

  }
}
