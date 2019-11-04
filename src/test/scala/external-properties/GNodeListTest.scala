import java.io.FileInputStream
import java.net.InetAddress
import java.util.Properties

import cn.graiph.cnode.{NodeAddress, ZKServiceRegistry}
import org.junit.{Assert, Test}
class GNodeListTest {



//  @Test
//  def testNodeAddress(): Unit ={
//    val ip = "10.0.88.99"
//    val port = 1234
//    val gnode = NodeAddress(ip,port)
//    Assert.assertEquals("10.0.88.99:1234",gnode.getNodeAddress())
//  }

  @Test
  def testProperties(): Unit = {
    val path = Thread.currentThread().getContextClassLoader.getResource("gNode.properties").getPath;
    val prop = new Properties()
    prop.load(new FileInputStream(path))
    Assert.assertEquals("10.0.86.26:2181",prop.getProperty("zkServerAddress"))
    Assert.assertEquals("159.226.193.204:7688",prop.getProperty("gNodeServiceAddress"))
    Assert.assertEquals("20000",prop.getProperty("sessionTimeout"))
  }

  @Test
  def testGetLocalIP(): Unit = {
    val localhostIP = InetAddress.getLocalHost().getHostAddress()
    Assert.assertEquals("159.226.193.204",localhostIP)
  }

  @Test
  def testCreateNodeAddress(): Unit = {
    val str = "10.0.88.99:1234"
    val nodeAddress = NodeAddress.fromString(str)
    println(nodeAddress)
  }

  @Test
  def testGetReadList(): Unit ={
    registerAsNode()
  }

  def registerAsNode(): Unit ={
    val register = new ZKServiceRegistry()
    register.registry("read","10.0.88.99:1111")
  }


}