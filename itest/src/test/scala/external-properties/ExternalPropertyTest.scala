package Externalproperties

import java.io.{File, FileInputStream}
import java.util.Properties

import cn.pandadb.externalprops.InMemoryPropertyNodeStore
import org.junit.{Assert, Test}
import org.neo4j.driver.{AuthTokens, GraphDatabase, Transaction, TransactionWork}
import org.neo4j.values.storable.Values

/**
 * Created by codeBabyLin on 2019/12/4.
 */


  //test CRUD


class ExternalPropertyTest {
  @Test
  def test1() {
    val transaction = InMemoryPropertyNodeStore.beginWriteTransaction()
    transaction.addNode(1)
    transaction.addProperty(1, "name", Values.of("bluejoe"))
    transaction.commit()
    transaction.close()
    //scalastyle:off println
    println(InMemoryPropertyNodeStore.nodes.get(1))
    //scalastyle:off println
  }
}
