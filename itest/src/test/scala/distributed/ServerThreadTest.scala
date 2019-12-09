package distributed

import java.util.concurrent.{ExecutorService, Executors}

import cn.pandadb.tool.PNodeServerStarter

class ThreadServer(num: Int) extends Runnable {
  override def run(): Unit = {
    //scalastyle:off
    PNodeServerStarter.main(Array(s"./itest/output/testdb/db${num}",
      s"./itest/testdata/gnode${num}.conf"));
    println(num)
  }
}

object  ServerThreadTest{

  def main(args: Array[String]) {
    //创建线程池
    val threadPool:ExecutorService=Executors.newFixedThreadPool(2)
    try {
      //提交5个线程
      for(i <- 0 to 1){
        //threadPool.submit(new ThreadDemo("thread"+i))
        threadPool.execute(new ThreadServer(i))
      }
    }finally {
      threadPool.shutdown()
    }
  }

}
