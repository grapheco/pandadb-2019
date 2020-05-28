package cn.pandadb.server.Store

import cn.pandadb.util.FileUtils

class DataStore(storeLayout: DataStoreLayout) {

  def getDataVersion(): Long = {
    val versionStore = storeLayout.localDataVersionStore
    val content = FileUtils.readTextFile(versionStore).trim
    if (!content.isEmpty) content.toLong
    else 0
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
    FileUtils.isEmptyDirectory(storeLayout.databaseDirectory)
  }

  def databaseDirectory: String = storeLayout.databaseDirectory.getAbsolutePath
}
