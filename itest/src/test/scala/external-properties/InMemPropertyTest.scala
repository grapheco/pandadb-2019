package Externalproperties

import java.io.{File, FileInputStream}
import java.util.Properties

import cn.pandadb.externalprops.InMemoryPropertyNodeStore
import org.junit.{Assert, Test}
import org.neo4j.driver.{AuthTokens, GraphDatabase, Transaction, TransactionWork}
import org.neo4j.values.AnyValues
import org.neo4j.values.storable.Values

/**
 * Created by codeBabyLin on 2019/12/4.
 */


  //test CRUD


class InMemPropertyTest {


  //test node add 、delete,and property add and remove
  @Test
  def test1() {
    InMemoryPropertyNodeStore.nodes.clear()
    Assert.assertEquals(0, InMemoryPropertyNodeStore.nodes.size)
    var transaction = InMemoryPropertyNodeStore.beginWriteTransaction()
    transaction.addNode(1)
    transaction.commit()
    Assert.assertEquals(1, InMemoryPropertyNodeStore.nodes.size)
    transaction = InMemoryPropertyNodeStore.beginWriteTransaction()
    transaction.addProperty(1, "name", Values.of("bluejoe"))
    var name = transaction.getPropertyValue(1, "name")
    Assert.assertEquals("bluejoe", name.get.asObject())

    transaction.removeProperty(1, "name")
    name = transaction.getPropertyValue(1, "name")
    Assert.assertEquals(None, name)

    transaction.deleteNode(1)
    transaction.commit()
    transaction.close()
    Assert.assertEquals(0, InMemoryPropertyNodeStore.nodes.size)

  }

  //test label add and removed
  @Test
  def test2() {
    InMemoryPropertyNodeStore.nodes.clear()
    Assert.assertEquals(0, InMemoryPropertyNodeStore.nodes.size)
    val transaction = InMemoryPropertyNodeStore.beginWriteTransaction()
    transaction.addNode(1)
    transaction.addLabel(1, "person")
    var label = transaction.getNodeLabels(1)

    Assert.assertEquals("person", label.head)

    transaction.removeLabel(1, "person")
    label = transaction.getNodeLabels(1)
    Assert.assertEquals(true, label.isEmpty)
    transaction.deleteNode(1)
    transaction.commit()
    transaction.close()
    Assert.assertEquals(0, InMemoryPropertyNodeStore.nodes.size)

  }


  @Test
  def test3() {
    InMemoryPropertyNodeStore.nodes.clear()
    Assert.assertEquals(0, InMemoryPropertyNodeStore.nodes.size)
    val transaction = InMemoryPropertyNodeStore.beginWriteTransaction()
    transaction.addNode(1)
    transaction.addLabel(1, "person")
    var label = transaction.getNodeLabels(1)

    Assert.assertEquals("person", label.head)

    val redo = transaction.commit()

    Assert.assertEquals(1, InMemoryPropertyNodeStore.nodes.size)
    Assert.assertEquals("person", InMemoryPropertyNodeStore.nodes.get(1).get.mutable().labels.head)

    redo.undo()
    transaction.commit()
    redo.undo()
    transaction.close()

    Assert.assertEquals(0, InMemoryPropertyNodeStore.nodes.size)

  }

  //test label add
  @Test
  def test4() {
    InMemoryPropertyNodeStore.nodes.clear()
    Assert.assertEquals(0, InMemoryPropertyNodeStore.nodes.size)
    val transaction = InMemoryPropertyNodeStore.beginWriteTransaction()
    transaction.addNode(1)
    transaction.addLabel(1, "person")
    var label = transaction.getNodeLabels(1)

    Assert.assertEquals("person", label.head)

    transaction.addLabel(1, "Man")
    label = transaction.getNodeLabels(1)
    assert(label.size == 2 && label.contains("Man") && label.contains("person"))

    transaction.commit()
    transaction.close()

    Assert.assertEquals(1, InMemoryPropertyNodeStore.nodes.size)
    val node = InMemoryPropertyNodeStore.getNodeById(1)
    val labels = node.get.mutable().labels
    assert(labels.size == 2 && labels.contains("person") && labels.contains("Man"))
  }


}
