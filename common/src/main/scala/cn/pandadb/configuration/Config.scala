package cn.pandadb.configuration

import java.io.{File, FileInputStream}
import java.util.Properties
import scala.collection.JavaConverters._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

object SettingKeys {
  val pandadbVersion = "db.version"
  val zkAddress = "zk.address"
  val zkPandaDBPath = "zk.pandadb.node.path"
  val rpcListenHost = "rpc.listen.host"
  val rpcListenPort = "rpc.listen.port"
  val rpcServerName = "rpc.server.name"
  val dataNodeRpcEndpointName = "rpc.endpoint.datanode.name"
  val leaderNodeRpcEndpointName = "rpc.endpoint.leadernode.name"
  val localNeo4jDBPath = "db.local.neo4j.path"
  val localDataStorePath = "db.local.data.path"

  val regionfsZKAddress = "blob.regionfs.zk.address"
}

class Config {
  private val settingsMap = new mutable.HashMap[String, String]()
  private val logger = getLogger(this.getClass)

  def withFile(configFile: Option[File]): Config = {
    if (configFile.isDefined) {
      val props = new Properties()
      props.load(new FileInputStream(configFile.get))
      settingsMap ++= props.asScala
    }
    this
  }

  def withSettings(settings: Map[String, String]): Config = {
    settingsMap ++= settings
    this
  }

  def validate(): Unit = {}

  def getPandadbVersion(): String = {
    getValueAsString(SettingKeys.pandadbVersion, "v0.0.0")
  }

  def getZKAddress(): String = {
    getValueAsString(SettingKeys.zkAddress, "127.0.0.1:2181")
  }
  def getPandaZKDir(): String = {
    getValueAsString(SettingKeys.zkPandaDBPath, s"/pandadb/${getPandadbVersion()}/")
  }

  def getListenHost(): String = {
    getValueAsString(SettingKeys.rpcListenHost, "127.0.0.1")
  }
  def getRpcPort(): Int = {
    getValueAsInt(SettingKeys.rpcListenPort, 52300)
  }
  def getNodeAddress(): String = {getListenHost + ":" + getRpcPort.toString}

  def getLocalNeo4jDatabasePath(): String = {
    getValueAsString(SettingKeys.localNeo4jDBPath, "/pandadb/db/graph.db")
  }
  def getRpcServerName(): String = {
    getValueAsString(SettingKeys.rpcServerName, "pandadb-server")
  }
  def getDataNodeEndpointName(): String = {
    getValueAsString(SettingKeys.dataNodeRpcEndpointName, "data-node-endpoint")
  }
  def getLeaderNodeEndpointName(): String = {
    getValueAsString(SettingKeys.leaderNodeRpcEndpointName, "leader-node-endpoint")
  }

  def getLogger(clazz: Class[_]): Logger = {
    LoggerFactory.getLogger(clazz)
  }

  def getRegionfsZkAddress(): String = {
    getValueAsString(SettingKeys.regionfsZKAddress, "127.0.0.1:2181")
  }

  def getLocalDataStorePath(): String = {
    getValueAsString(SettingKeys.localDataStorePath, "/pandadb/data")
  }

  private def getValueWithDefault[T](key: String, defaultValue: () => T, convert: (String) => T)(implicit m: Manifest[T]): T = {
    val opt = settingsMap.get(key);
    if (opt.isEmpty) {
      val value = defaultValue();
      logger.debug(s"no value set for $key, using default: $value");
      value;
    }
    else {
      val value = opt.get;
      try {
        convert(value);
      }
      catch {
        case e: java.lang.IllegalArgumentException =>
          throw new WrongArgumentException(key, value, m.runtimeClass);
      }
    }
  }

  private def getValueAsString(key: String, defaultValue: String): String =
    getValueWithDefault(key, () => defaultValue, (x: String) => x)

  private def getValueAsClass(key: String, defaultValue: Class[_]): Class[_] =
    getValueWithDefault(key, () => defaultValue, (x: String) => Class.forName(x))

  private def getValueAsInt(key: String, defaultValue: Int): Int =
    getValueWithDefault[Int](key, () => defaultValue, (x: String) => x.toInt)

  private def getValueAsBoolean(key: String, defaultValue: Boolean): Boolean =
    getValueWithDefault[Boolean](key, () => defaultValue, (x: String) => x.toBoolean)

}


class ArgumentRequiredException(key: String) extends
  RuntimeException(s"argument required: $key") {

}

class WrongArgumentException(key: String, value: String, clazz: Class[_]) extends
  RuntimeException(s"wrong argument: $key, value=$value, expected: $clazz") {

}
