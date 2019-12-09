package distributed

import java.io.{ObjectInputStream, ObjectOutputStream}
import java.net.Socket

import scala.collection.mutable.ArrayBuffer



trait SayHello{
  def hello(name: String)
}

trait LogDetail{
  def getLog(version: Int): String
}

class WriteCypherImpl() {
  def printLog(statement: String): Unit = {
    //scalastyle:off println
    println(statement)  //here to do something
    //scalastyle:on println
  }
}


object RpcClient {
  def main(args: Array[String]) {
    val log = new LogDetailRemoteImpl()
    val wc = new WriteCypherImpl()
    wc.printLog(log.getLog(2))
  }

}


class LogDetailRemoteImpl extends LogDetail {
  override def getLog(version: Int): String = {
    val addressList = lookupProviders("Calculator.add");
    val address = addressList.head;
    val PORT = 9090;
    try {
      val socket = new Socket(address, PORT);

      // 将请求序列化

      val objectOutputStream = new ObjectOutputStream(socket.getOutputStream());

      // 将请求发给服务提供方
      objectOutputStream.writeObject(version.toString);

      // 将响应体反序列化
      val objectInputStream = new ObjectInputStream(socket.getInputStream());
      val response = objectInputStream.readObject();

      response.toString
    }
  }

  def lookupProviders(name: String): ArrayBuffer[String] = {
    var strings = new ArrayBuffer[String]();
    strings += "127.0.0.1";
    strings;
  }
}


