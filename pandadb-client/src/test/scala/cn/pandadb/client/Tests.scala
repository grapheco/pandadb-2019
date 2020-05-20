package cn.pandadb.client

import org.junit.Test

class Tests {

  val client = new PandaDBClient("127.0.0.1:2181")

  @Test
  def test1(): Unit = {
//    val res1 = client.createNode(Array("Person"), Map("name"->"t1", "age"->10))
//    println(res1)
//    val res2 = client.createNode(Array("Person", "boy"), Map("name"->"t2", "age"->15))
//    val res3 = client.createNode(Array("Person", "boy"), Map("name"->"t3", "bb"->15))
//    println(res1)
    val res4 = client.runCypher("match (n:Person) return n")
    println(res4.records.size)
    println(res4)
  }

}
