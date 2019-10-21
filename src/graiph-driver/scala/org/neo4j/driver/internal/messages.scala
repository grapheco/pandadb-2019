package org.neo4j.driver.internal

import java.util.concurrent.CompletableFuture

import org.neo4j.blob.BlobMessageSignature
import org.neo4j.blob.utils.Logging
import org.neo4j.driver.Value
import org.neo4j.driver.internal.messaging.{Message, MessageEncoder, ValuePacker}
import org.neo4j.driver.internal.spi.ResponseHandler
import org.neo4j.driver.internal.util.Preconditions._
import org.neo4j.driver.internal.value.BlobChunk

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bluejoe on 2019/4/18.
  */

class GetBlobMessage(val blodId: String) extends Message {
  def signature: Byte = {
    return BlobMessageSignature.SIGNATURE_GET_BLOB;
  }

  override def toString: String = {
    return "GetBlob"
  }
}

class GetBlobMessageEncoder extends MessageEncoder {
  override def encode(message: Message, packer: ValuePacker): Unit = {
    checkArgument(message, classOf[GetBlobMessage])
    packer.packStructHeader(1, BlobMessageSignature.SIGNATURE_GET_BLOB)
    packer.pack((message.asInstanceOf[GetBlobMessage]).blodId)
  }
}

class GetBlobMessageHandler(report: CompletableFuture[(BlobChunk, ArrayBuffer[CompletableFuture[BlobChunk]])], exception: CompletableFuture[Throwable])
  extends ResponseHandler with Logging {
  val _chunks = ArrayBuffer[CompletableFuture[BlobChunk]]();
  var _completedIndex = -1;

  override def onSuccess(metadata: java.util.Map[String, Value]): Unit = {
    exception.complete(null);
  }

  override def onRecord(fields: Array[Value]): Unit = {
    val chunk = new BlobChunk(
      fields(0).asInt(),
      fields(1).asInt(),
      fields(2).asInt(),
      fields(3).asByteArray(),
      fields(4).asBoolean(),
      fields(5).asInt());

    _completedIndex += 1;
    if (_chunks.size < _completedIndex + 5) {
      _chunks ++= (0 to 9).map(_ => new CompletableFuture[BlobChunk]());
    }

    _chunks(_completedIndex).complete(chunk);

    //first!
    if (chunk.chunkId == 0) {
      report.complete(chunk, _chunks);
    }
  }

  override def onFailure(error: Throwable): Unit = {
    exception.complete(error);

    if (!report.isDone)
      report.complete(null);

    _chunks.foreach { x =>
      if (!x.isDone)
        x.complete(null)
    }
  }
}