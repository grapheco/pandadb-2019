package distributed

import java.io.{File, FileInputStream}
import java.util.{Locale, Properties}

import cn.pandadb.driver.PandaDriver
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

object PandaDriverTest {
  val configFile = new File("./testdata/gnode0.conf")
  val props = new Properties()
  props.load(new FileInputStream(configFile))
  val pandaString = s"panda://" + props.getProperty("zkServerAddress") + s"/db"

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
    pandaDriver.session().run("Match(n) Delete n;")
    val clusterResult = pandaDriver.session().run("Match(n) Return n;")
    val node0Result = neoDriver0.session().run("Match(n) Delete n;")
    val node1Result = neoDriver1.session().run("Match(n) Delete n;")
    Assert.assertEquals(false, clusterResult.hasNext)
    Assert.assertEquals(false, node0Result.hasNext)
    Assert.assertEquals(false, node1Result.hasNext)
  }

  @Test
  def test2(): Unit = {
    pandaDriver.session().run("Create(n:Test{prop:'panda'})")
    _createAndMerge()
  }

  @Test
  def test3(): Unit = {
    pandaDriver.session().run("Merge(n:Test{prop:'panda'})")
    _createAndMerge()
  }

  @Test
  def test4(): Unit = {
    pandaDriver.session().run("Create(n:Test{prop:'panda'})")
    pandaDriver.session().run("Merge(n:Test{prop:'panda'})")
    _createAndMerge()
  }

  @Test
  def test5(): Unit = {
    pandaDriver.session().run("Create(n:Test)")
    val clusterResult1 = pandaDriver.session().run("Match(n) return n;")
    Assert.assertEquals(true, clusterResult1.hasNext)
    Assert.assertEquals(null, clusterResult1.next().get("n").asNode().get("prop"))
    Assert.assertEquals(false, clusterResult1.hasNext)

    pandaDriver.session().run("Match(n) Set n.prop='panda'")
    val clusterResult2 = pandaDriver.session().run("Match(n) return n;")
    Assert.assertEquals(true, clusterResult1.hasNext)
    Assert.assertEquals(null, clusterResult1.next().get("n").asNode().get("prop").asString())
    Assert.assertEquals(false, clusterResult2.hasNext)

    pandaDriver.session().run("Match(n) delete n;")
    val clusterResult3 = pandaDriver.session().run("Match(n) Return n;")
    Assert.assertEquals(false, clusterResult3.hasNext)
  }

  //add a test, to test different menthod to run statement.


  private def _createAndMerge(): Unit = {

//    val clusterResult = pandaDriver.session().run("Match(n) Where n.prop='panda' Return n")
//    val node0Result = neoDriver0.session().run("Match(n) Where n.prop='panda' Return n")
//    val node1Result = neoDriver1.session().run("Match(n) Where n.prop='panda' Return n")
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

    pandaDriver.session().run("Match(n) Delete n;")
    val clusterResult1 = pandaDriver.session().run("Match(n) Where n.prop='panda' Return n")
    val node0Result1 = neoDriver0.session().run("Match(n) Where n.prop='panda' Return n")
    val node1Result1 = neoDriver1.session().run("Match(n) Where n.prop='panda' Return n")
    Assert.assertEquals(false, clusterResult1.hasNext)
    Assert.assertEquals(false, node0Result1.hasNext)
    Assert.assertEquals(false, node1Result1.hasNext)
  }

}
