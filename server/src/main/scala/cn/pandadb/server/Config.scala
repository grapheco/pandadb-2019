package cn.pandadb.server

import java.io.File


class Config {
  private var zkAddress: Option[String] = None
  private var rpcPort: Option[Int] = Option(52345)
  private val pandaZKDir: String = "/pandadb/v0.0.3/"
  private val listenHost: String = "localhost"

  def withFile(configFile: Option[File]): Config = { this }

  def withSettings(settings: Map[String, String]): Config = {this}

  def validate(): Unit = {}

  def getZKAddress(): String = {zkAddress.get}
  def getPandaZKDir: String = {pandaZKDir}

  def getListenHost(): String = {listenHost}
  def getRpcPort: Int = {rpcPort.get}

}
