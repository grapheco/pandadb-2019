package distributed

import java.io.{ObjectInputStream, ObjectOutputStream}
import java.net.ServerSocket

import org.apache.spark.network.protocol.RpcRequest
import org.slf4j.LoggerFactory




object RpcServer {
  def main(args: Array[String]) {
    val server = new RpcServer()

    server.run()
  }

}



class RpcServer {

  private val logHandle = new LogDetailImpl()

  def run() {
    val listener = new ServerSocket(9090);
    try {
      while (true) {
        val socket = listener.accept();
        try {
          // 将请求反序列化
          val objectInputStream = new ObjectInputStream(socket.getInputStream());
          val version = objectInputStream.readObject().toString.toInt;



          // 调用服务
          var result: String = null;
          if (version != null) {
            //val test = obj.asInstanceOf[RpcRequest2]

            result = logHandle.getLog(version)
          }
          val objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
          objectOutputStream.writeObject(result);
        }


      }


    }
  }

}

class LogDetailImpl extends LogDetail {
  override def getLog(version: Int): String = {
    var res: String = null
    version match {
      case 1 => res = "hello world!!!"
      case 2 => res = "pandaDb"
      case _ => res = "error"
    }
    res
  }
}