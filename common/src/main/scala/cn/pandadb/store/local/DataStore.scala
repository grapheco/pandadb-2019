package cn.pandadb.store.local

import java.io.File

import cn.pandadb.util.FileUtils

class DataStore(storeLayout: DataStoreLayout) {
  val ZERO_DATA_VERSION = 0L

  def dataVersionStore: File = storeLayout.localDataVersionStore

  def getDataVersion(): Long = {
    val versionStore = storeLayout.localDataVersionStore
    val content = FileUtils.readTextFile(versionStore).trim
    if (!content.isEmpty) content.toLong
    else {
      FileUtils.writeToFile(versionStore, ZERO_DATA_VERSION.toString, false)
      ZERO_DATA_VERSION
    }
  }

  def setDataVersion(version: Long): Unit = {
    FileUtils.writeToFile(storeLayout.localDataVersionStore, version.toString, false)
  }

  def getDataLog(): String = {
    FileUtils.readTextFile(storeLayout.localDataLogStore)
  }

  def appendDataLog(log: String): Unit = {
    FileUtils.writeToFile(storeLayout.localDataLogStore, log, true)
  }

  def isDatabaseEmpty(): Boolean = {
    FileUtils.isEmptyDirectory(storeLayout.graphStoreDirectory)
  }

  def graphStoreDirectory: String = storeLayout.graphStoreDirectory.getAbsolutePath

  def graphStore: File = storeLayout.graphStoreDirectory
}
