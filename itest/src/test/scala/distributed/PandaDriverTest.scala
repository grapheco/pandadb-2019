package distributed

import java.io.{File, FileInputStream}
import java.util.{Locale, Properties}

import cn.pandadb.driver.PandaDriver
import cn.pandadb.network.ZKPathConfig
import distributed.PandaDriverTest.{neoDriver0, neoDriver1, pandaDriver}
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.junit.runners.MethodSorters
import org.junit._
import org.neo4j.driver.{AuthTokens, GraphDatabase}

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created at 16:58 2019/12/7
  * @Modified By:
  */

// todo: test n.prop
object PandaDriverTest {
  val configFile = new File("./testdata/gnode0.conf")
  val props = new Properties()
  props.load(new FileInputStream(configFile))
  val pandaString = s"panda://" + props.getProperty("zkServerAddress") + s"/db"

  ZKPathConfig.initZKPath(props.getProperty("zkServerAddress"))
  // correct these two addresses please
  val node0 = "bolt://localhost:7684"
  val node1 = "bolt://localhost:7685"

  val pandaDriver = GraphDatabase.driver(pandaString, AuthTokens.basic("", ""))
  val neoDriver0 = GraphDatabase.driver(node0, AuthTokens.basic("", ""))
  val neoDriver1 = GraphDatabase.driver(node1, AuthTokens.basic("", ""))

  @BeforeClass
  def deleteAllData(): Unit = {
    pandaDriver.session().run("Match(n) Delete n;")
    Thread.sleep(1500)
  }

  @AfterClass
  def deleteVersion(): Unit = {
    pandaDriver.session().run("Match(n) Delete n;")
    val curator = CuratorFrameworkFactory.newClient(props.getProperty("zkServerAddress"),
      new ExponentialBackoffRetry(1000, 3))
    curator.start()
    curator.delete().forPath("/testPandaDB/version")
    curator.close()
  }
}

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
class PandaDriverTest {

  // check the type
  @Test
  def test0(): Unit = {
    // scalastyle:off
    Assert.assertEquals(true, pandaDriver.isInstanceOf[PandaDriver])
    Assert.assertEquals("class org.neo4j.driver.internal.InternalDriver", neoDriver0.getClass.toString)
    Assert.assertEquals("class org.neo4j.driver.internal.InternalDriver", neoDriver1.getClass.toString)
  }

  // make sure the database is blank
  @Test
  def test1(): Unit = {
    val session = pandaDriver.session()
    val tx = session.beginTransaction()
    tx.run("Match(n) Delete n;")
    tx.success()
    tx.close()
    session.close()
    val clusterResult = pandaDriver.session().run("Match(n) Return n;")
    val node0Result = neoDriver0.session().run("Match(n) Return n;")
    val node1Result = neoDriver1.session().run("Match(n) Return n;")
    Assert.assertEquals(false, clusterResult.hasNext)
    Assert.assertEquals(false, node0Result.hasNext)
    Assert.assertEquals(false, node1Result.hasNext)
  }

  @Test
  def test2(): Unit = {
    val session = pandaDriver.session()
    val tx = session.beginTransaction()
    tx.run("Create(n:Test{prop:'panda'})")
    tx.success()
    tx.close()
    session.close()
    _createAndMerge()
  }

  @Test
  def test3(): Unit = {
    val session = pandaDriver.session()
    val tx = session.beginTransaction()
    tx.run("Merge(n:Test{prop:'panda'})")
    tx.success()
    tx.close()
    session.close()
    _createAndMerge()
  }

  @Test
  def test4(): Unit = {

    val session = pandaDriver.session()
    val tx = session.beginTransaction()
    tx.run("Create(n:Test{prop:'panda'})")
    tx.success()
    tx.close()
    session.close()

    val session1 = pandaDriver.session()
    val tx1 = session1.beginTransaction()
    tx1.run("Merge(n:Test{prop:'panda'})")
    tx1.close()
    session1.close()
    _createAndMerge()
  }

  @Test
  def test5(): Unit = {
    val session = pandaDriver.session()
    val tx = session.beginTransaction()
    tx.run("Create(n:Test)")
    tx.success()
    tx.close()
    session.close()

    val session1 = pandaDriver.session()
    val tx1 = session1.beginTransaction()
    tx1.run("Match(n) Set n.prop='panda'")
    tx1.success()
    tx1.close()
    session1.close()

    _createAndMerge()
  }

  //add a test, to test different menthod to run statement.
  private def _createAndMerge(): Unit = {
    // Problem: the result is not available real-time.
    val clusterResult = pandaDriver.session().run("Match(n) Return n")
    val node0Result = neoDriver0.session().run("Match(n) Return n")
    val node1Result = neoDriver1.session().run("Match(n) Return n")
    Assert.assertEquals(true, clusterResult.hasNext)
    Assert.assertEquals("panda", clusterResult.next().get("n").asNode().get("prop").asString())
    Assert.assertEquals(false, clusterResult.hasNext)

    Assert.assertEquals(true, node0Result.hasNext)
    Assert.assertEquals("panda", node0Result.next().get("n").asNode().get("prop").asString())
    Assert.assertEquals(false, node0Result.hasNext)

    Assert.assertEquals(true, node1Result.hasNext)
    Assert.assertEquals("panda", node1Result.next().get("n").asNode().get("prop").asString())
    Assert.assertEquals(false, node1Result.hasNext)

    val session = pandaDriver.session()
    val tx = session.beginTransaction()
    tx.run("Match(n) Delete n;")
    tx.success()
    tx.close()
    session.close()
    val clusterResult1 = pandaDriver.session().run("Match(n) Return n")
    val node0Result1 = neoDriver0.session().run("Match(n) Return n")
    val node1Result1 = neoDriver1.session().run("Match(n) Return n")
    Assert.assertEquals(false, clusterResult1.hasNext)
    Assert.assertEquals(false, node0Result1.hasNext)
    Assert.assertEquals(false, node1Result1.hasNext)
  }

}
