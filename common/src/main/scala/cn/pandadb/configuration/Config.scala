package cn.pandadb.configuration

import java.io.File
import org.slf4j.{Logger, LoggerFactory}

class Config {
  private var zkAddress: Option[String] = Option("127.0.0.1:2181")
  private var rpcPort: Option[Int] = Option(52345)
  private val pandaZKDir: String = "/pandadb/v0.0.3/"
  private val listenHost: String = "localhost"
  private val localNeo4jDatabasePath = "output1/testdb1"
  private val rpcServerName = "data-node-server"
  private val dataNodeRpcEndpointName = "data-node-endpoint"
  private val leaderNodeRpcEndpointName = "leader-node-endpoint"

  def withFile(configFile: Option[File]): Config = { this }

  def withSettings(settings: Map[String, String]): Config = {this}

  def validate(): Unit = {}

  def getZKAddress(): String = {zkAddress.get}
  def getPandaZKDir: String = {pandaZKDir}

  def getListenHost(): String = {listenHost}
  def getRpcPort(): Int = {rpcPort.get}
  def getNodeAddress(): String = {getListenHost + ":" + getRpcPort.toString}

  def getLocalNeo4jDatabasePath(): String = {localNeo4jDatabasePath}
  def getRpcServerName(): String = {rpcServerName}
  def getDataNodeEndpointName(): String = {dataNodeRpcEndpointName}
  def getLeaderNodeEndpointName(): String = {leaderNodeRpcEndpointName}

  def getLogger(clazz: Class[_]): Logger = {
    LoggerFactory.getLogger(clazz)
  }

}
