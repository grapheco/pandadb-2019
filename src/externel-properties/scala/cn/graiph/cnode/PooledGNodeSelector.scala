package cn.graiph.cnode

import org.neo4j.driver.{AuthTokens, Driver, GraphDatabase}

import scala.collection.mutable
import scala.util.Random

/**
  * Created by bluejoe on 2019/11/5.
  */

class PooledGNodeSelector extends GNodeSelector with GNodeListListener {
  val rand = new Random();

  override def chooseReadNode(): Driver = cachedReadDrivers.values.toSeq.apply(rand.nextInt(cachedReadDrivers.size))

  override def chooseWriteNode(): Driver = cachedWriteDrivers.values.toSeq.apply(rand.nextInt(cachedWriteDrivers.size))

  val cachedReadDrivers = mutable.Map[NodeAddress, Driver]();

  val cachedWriteDrivers = mutable.Map[NodeAddress, Driver]();

  override def onEvent(event: GNodeListEvent): Unit = {
    event match {
      case ReadGNodeConnected(address) =>
        cachedReadDrivers += address -> createDriver(address);

      case WriteGNodeConnected(address) =>
        cachedWriteDrivers += address -> createDriver(address);

      case ReadGNodeDisconnected(address) => {
        cachedReadDrivers(address).close()
        cachedReadDrivers -= address
      }

      case WriteGNodeDisconnected(address) => {
        cachedWriteDrivers(address).close()
        cachedWriteDrivers -= address
      }
    }
  }

  def createDriver(address: NodeAddress): Driver = {
    val url = "";
    val driver = GraphDatabase.driver(url, AuthTokens.basic("", ""));
    driver;
  }
}
