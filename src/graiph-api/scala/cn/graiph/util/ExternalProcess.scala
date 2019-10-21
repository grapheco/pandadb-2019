package cn.graiph.util

import java.io.ByteArrayOutputStream
import org.neo4j.blob.utils.StreamUtils._
import org.apache.commons.io.IOUtils
import org.neo4j.blob.Blob
import org.neo4j.blob.utils.Logging
import scala.util.parsing.json.JSON

/**
  * Created by bluejoe on 2018/12/11.
  */
object ExternalProcess extends Logging {
  def submit(command: String, blobs: Blob*): Map[String, Any] = {
    val process: Process = Runtime.getRuntime().exec(command.split(" "));
    val pos = process.getOutputStream;
    val pis = process.getInputStream;

    val baos = new ByteArrayOutputStream();
    var isProcessTerminated = false;
    val outputCollectorThread = new Thread() {
      override def run() {
        while (!isProcessTerminated) {
          val buffer = new Array[Byte](1024);
          val count = pis.read(buffer);
          if (count > 0)
            baos.write(buffer, 0, count);
        }
      }
    };

    val inputReceiverThread = new Thread() {
      override def run() {
        blobs.foreach { blob =>
          pos.writeLong(blob.length);
          pos.flush();

          blob.offerStream { (is) =>
            IOUtils.copy(is, pos);
            pos.flush();
            null;
          }
        }

        pos.write(-1); //end
        pos.flush();
      }
    };

    outputCollectorThread.start();
    inputReceiverThread.start();
    process.waitFor();
    isProcessTerminated = true;

    val ot = new String(baos.toByteArray, "utf-8");
    logger.debug(s"process output: \r\n$ot");
    JSON.parseFull(ot).get.asInstanceOf[Map[String, Any]];
  }
}