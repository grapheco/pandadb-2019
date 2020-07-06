import cn.pandadb.jraft.PandaJraftClient
import org.junit.Test
// scalastyle:off println
class JraftTest {
  @Test
  def pandaClientTest(): Unit = {
    val groupId = "pandaserver"
    val initConfStr = "127.0.0.1:6061,127.0.0.1:6062,127.0.0.1:6063"
    val client = new PandaJraftClient(groupId, initConfStr)
    //val cypher = "create(n:person{name:'test'})"
   // val cypher2 = "match (n:person) return n"
   // val c1 = client.runCypher(cypher).columns().size()
   // val c2 = client.runCypher(cypher2).columns().size()
   // println(c1)
   // println(c2)
  }

}
