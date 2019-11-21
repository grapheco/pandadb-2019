package cn.pandadb.cnode

import cn.pandadb.network.NodeAddress
import org.neo4j.driver.{AuthTokens, Driver, GraphDatabase}

import scala.collection.mutable
import scala.util.Random

/**
  * Created by bluejoe on 2019/11/5.
  */

class PooledGNodeSelector extends GNodeSelector with GNodeListListener {
  val rand = new Random();

  // TO DO： Fix the selection method (workload balance or so on).
  override def chooseReadNode(): Driver = cachedReadDrivers.values.toSeq.apply(rand.nextInt(cachedReadDrivers.size))

  override def chooseWriteNode(): Driver = cachedWriteDrivers.values.toSeq.apply(rand.nextInt(cachedWriteDrivers.size))

  override def chooseAllNodes(): List[Driver] = cachedReadDrivers.values.toList ++: cachedWriteDrivers.values.toList

  val cachedReadDrivers = mutable.Map[NodeAddress, Driver]();

  val cachedWriteDrivers = mutable.Map[NodeAddress, Driver]();

  override def onEvent(event: GNodeListEvent): Unit = {
    event match {
      case ReadGNodeConnected(address) =>
        cachedReadDrivers += address -> createDriver(address);
      case WriteGNodeConnected(address) =>
        cachedWriteDrivers += address -> createDriver(address);
      case ReadGNodeDisconnected(address) =>
        cachedReadDrivers -= address;
      case WriteGNodeDisconnected(address) =>
        cachedWriteDrivers -= address;
    }
  }


  def createDriver(address: NodeAddress): Driver = {
    //get url(bolt://ip:port) from the address
    val url = s"bolt://${address.host}:${address.port}";
    val driver = GraphDatabase.driver(url, AuthTokens.basic("", ""));
    driver;
  }
}
