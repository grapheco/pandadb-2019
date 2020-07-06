object Lo4jTest {
  def main( args: Array[String] ): Unit = {
    Class.forName("org.slf4j.impl.Log4jLoggerFactory")
    Class.forName("org.apache.log4j.Log4jLoggerFactory");
  }
}
