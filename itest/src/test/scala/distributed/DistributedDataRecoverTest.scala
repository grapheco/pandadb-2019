package distributed

import cn.pandadb.network.ZKPathConfig
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.junit.{Assert, BeforeClass, Test}
import org.neo4j.driver.{AuthTokens, GraphDatabase}

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created at 17:37 2019/12/4
  * @Modified By:
  */

object DistributedDataRecoverTest {
  @BeforeClass
  val zkString = "10.0.86.26:2181,10.0.86.27:2181,10.0.86.70:2181"
  val pandaString = s"panda://" + zkString + "/db"
  val driver = GraphDatabase.driver(pandaString, AuthTokens.basic("", ""))
  val curator: CuratorFramework = CuratorFrameworkFactory.newClient(zkString,
    new ExponentialBackoffRetry(1000, 3));

  val node0 = s"bolt://10.0.87.7:7685"
  val zkMasterPath = ZKPathConfig.leaderNodePath + s"/10.0.87.7:7685"

  val node2 = s"bolt://10.0.87.9:7685"
  val zkSlavePath = ZKPathConfig.ordinaryNodesPath + s"/10.0.87.9:7685"

}

class DistributedDataRecoverTest {

  val time = "12:41"
  val time2 = "12:42"
  val time3 = "12:43"

  // only start node0
  @Test
  def test1(): Unit = {
    // no data in cluster
    val result = DistributedDataRecoverTest.driver.session().run("Match(n) Return n;")
    Assert.assertEquals(false, result.hasNext)

    // create a node
    val cypher = s"Create(n:Test{time:'${time}'});"
    DistributedDataRecoverTest.driver.session()
      .run(cypher)

    // query by panda driver
    val clusterResult = DistributedDataRecoverTest.driver.session().run("Match(n) Return n;")
    val t = clusterResult.next()
    Assert.assertEquals(time.toString, clusterResult.next().get("n").get("time").asString())

    // query by neo4j driver
    val nResult1 = GraphDatabase.driver(DistributedDataRecoverTest.node0)
      .session()
      .run("Match(n) Return n;")
    Assert.assertEquals(time.toString, nResult1.next().get("n").get("time").asString())

    // slave node hasn't started.
    val exists = DistributedDataRecoverTest.curator.checkExists()
      .forPath(DistributedDataRecoverTest.zkSlavePath)
    Assert.assertEquals(null, exists)

    // show the master dataVersionLog
  }

  // run the slave node here
  @Test
  def test2(): Unit = {

    // slave data is updated
    val slaveNode = GraphDatabase.driver(DistributedDataRecoverTest.node2)
    val slaveResult = slaveNode.session.run("Match(n) Return n;")
    Assert.assertEquals(time, slaveResult.next().get("n.time").asString())
  }

  @Test
  def test3(): Unit = {
    // create a new node
    DistributedDataRecoverTest.driver.session()
      .run(s"Create(n:Test2{time2:'${time2}'})")

    // only one node created.
    val clusterResult = DistributedDataRecoverTest.driver.session()
      .run(s"Match(n) Where n.time2='${time2}'")
    Assert.assertEquals(true, clusterResult.hasNext)
    Assert.assertEquals(time2, clusterResult.next().get("n.time2"))
    Assert.assertEquals(false, clusterResult.hasNext)
  }

  // close slave here.
  @Test
  def test4(): Unit = {
    DistributedDataRecoverTest.driver.session().run(s"Match(n) Delete n;")
    DistributedDataRecoverTest.driver.session().run(s"Create(n:Test3);")
    DistributedDataRecoverTest.driver.session().run(s"Match(n) Set n.time3 = '${time3}';")

    val clusterResult = DistributedDataRecoverTest.driver.session().run(s"Match(n) Return n;")
    Assert.assertEquals(time3, clusterResult.next().get("n.time3"))
  }

  // close master here.
  // no server in the cluster now
  // start slave here.
  @Test
  def test5(): Unit = {
    Assert.assertEquals(null,
      DistributedDataRecoverTest.curator.checkExists().forPath(DistributedDataRecoverTest.zkSlavePath))

    Assert.assertEquals(null,
      DistributedDataRecoverTest.curator.checkExists().forPath(DistributedDataRecoverTest.zkMasterPath))
  }

  // start master here.
  @Test
  def test6(): Unit = {

    Assert.assertEquals(false,
      DistributedDataRecoverTest.curator.checkExists().forPath(DistributedDataRecoverTest.zkSlavePath) == null)

    Assert.assertEquals(false,
      DistributedDataRecoverTest.curator.checkExists().forPath(DistributedDataRecoverTest.zkMasterPath) == null)

    val clusterResult = DistributedDataRecoverTest.driver.session().run(s"Match(n) Return n;")
    Assert.assertEquals(time3, clusterResult.next().get("n.time3"))

    val masterResult = GraphDatabase.driver(DistributedDataRecoverTest.node0).session().run(s"Match(n) Return n;")
    Assert.assertEquals(time3, masterResult.next().get("n.time3"))

    val slaveResult = GraphDatabase.driver(DistributedDataRecoverTest.node0).session().run(s"Match(n) Return n;")
    Assert.assertEquals(time3, slaveResult.next().get("n.time3"))
  }

}
