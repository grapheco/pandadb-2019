package cn.pandadb.store.local

import java.io.File

object StoreFileNames {
  val LOCAL_DATA_VERSION = "local.data.version"
  val LOCAL_DATA_LOG = "local.data.log"
  val DATABASE_NAME = "graph.db"
}

class DataStoreLayout(storeDirectory: File) {
  val graphDatabaseName: String = StoreFileNames.DATABASE_NAME
  val graphStoreDirectory: File = file(StoreFileNames.DATABASE_NAME)
  val localDataVersionStore: File = file(StoreFileNames.LOCAL_DATA_VERSION)
  val localDataLogStore: File = file(StoreFileNames.LOCAL_DATA_LOG)

  private def file(fileName: String): File = new File(storeDirectory, fileName)

  private def layout(): DataStoreLayout = {
    if (!storeDirectory.exists()) {
      storeDirectory.mkdirs()
    }
    if (!graphStoreDirectory.exists()) {
      graphStoreDirectory.mkdirs()
    }
    if (!localDataVersionStore.exists()) {
      localDataVersionStore.createNewFile()
    }
    this
  }
}

object DataStoreLayout {
  def of(dataStoreDirectory: File): DataStoreLayout = (new DataStoreLayout(dataStoreDirectory)).layout()
}
