package distributed

import java.io.{File, FileInputStream}
import java.util.Properties

import org.junit.{Assert, Test}
import org.neo4j.driver.{AuthTokens, GraphDatabase, Transaction, TransactionWork}

/**
  * Created by bluejoe on 2019/11/21.
  */
class DriverTest {

  val configFile = new File("./testdata/gnode0.conf")
  val props = new Properties()
  props.load(new FileInputStream(configFile))
  val pandaString = s"panda://" + props.getProperty("zkServerAddress") + s"/db"


  @Test
  def test0() {
    val driver = GraphDatabase.driver(pandaString,
      AuthTokens.basic("", ""));
    var results1 = driver.session().run("create (n:person{name:'bluejoe'})");
    val results = driver.session().run("match (n:person) return n");

    val result = results.next();
    Assert.assertEquals("bluejoe", result.get("n").asNode().get("name").asString());
    val results2 = driver.session().run("match (n:person) delete n");
    driver.close();
  }
  @Test
  def test1() {
    val driver = GraphDatabase.driver(pandaString,
      AuthTokens.basic("", ""));
    val session = driver.session();
    var results1 = session.run("create (n:person{name:'bluejoe'})");
    val results = session.run("match (n:person) return n");
    val result = results.next();
    Assert.assertEquals("bluejoe", result.get("n").asNode().get("name").asString());
    val results2 = session.run("match (n:person) delete n");
    session.close();
    driver.close();
  }

  //test transaction
  @Test
  def test2() {
    val driver = GraphDatabase.driver(pandaString,
      AuthTokens.basic("", ""));
    val session = driver.session();
    val transaction = session.beginTransaction()
    var results1 = transaction.run("create (n:person{name:'bluejoe'})");

    results1 = transaction.run("create (n:people{name:'lin'})");
    val results = transaction.run("match (n:person) return n.name");

    val result = results.next();
    Assert.assertEquals("bluejoe", result.get("n.name").asString());

    val results3 = transaction.run("match (n) return n.name");
    Assert.assertEquals(2, results3.list().size())

    val results2 = transaction.run("match (n) delete n");

    Assert.assertEquals(0, results2.list().size())

    transaction.success()
    transaction.close()
    session.close();
    driver.close();
  }

  @Test
  def test3() {
    val driver = GraphDatabase.driver(pandaString,
      AuthTokens.basic("", ""));
    var session = driver.session()


    val result = session.writeTransaction(new TransactionWork[Unit] {
      override def execute(transaction: Transaction): Unit = {
        val res1 = transaction.run("create (n:person{name:'bluejoe'})")
      }
    })
    //session = driver.session()
    val result2 = session.readTransaction(new TransactionWork[Unit] {
      override def execute(transaction: Transaction): Unit = {
        val res2 = transaction.run("match (n:person) return n.name")
        Assert.assertEquals("bluejoe", res2.next().get("n.name").asString());
      }
    })
    //session = driver.session()
    val result3 = session.writeTransaction(new TransactionWork[Unit] {
      override def execute(transaction: Transaction): Unit = {
        val res3 = transaction.run("match (n) delete n")
        //Assert.assertEquals("bluejoe", res2.next().get("name").asString());
      }
    })
    session.close();
    driver.close();
  }


}
