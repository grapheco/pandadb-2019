package org.neo4j.driver.internal.util

import java.util

import org.neo4j.blob._
import org.neo4j.blob.utils.ReflectUtils._
import org.neo4j.blob.utils._
import org.neo4j.driver.Value
import org.neo4j.driver.internal.spi.Connection
import org.neo4j.driver.internal.value.{RemoteBlob, InternalBlobValue}

/**
  * Created by bluejoe on 2019/4/18.
  */
object BoltClientBlobIO {

  def unpackBlob(unpacker: org.neo4j.driver.internal.packstream.PackStream.Unpacker): Value = {
    val in = unpacker._get("in").asInstanceOf[org.neo4j.driver.internal.packstream.PackInput];
    val byte = in.peekByte();

    byte match {
      case BlobIO.BOLT_VALUE_TYPE_BLOB_REMOTE =>
        in.readByte();

        val values = for (i <- 0 to 3) yield in.readLong();
        val entry = BlobIO.unpack(values.toArray);

        val idlen = in.readInt();
        val bs = new Array[Byte](idlen);
        in.readBytes(bs, 0, idlen);

        val remoteHandle = new String(bs, "utf-8");

        val conn = in._get("_inboundMessageHandler.messageDispatcher.handlers")
          .asInstanceOf[util.LinkedList[_]].get(0)
          .asInstanceOf[AnyRef]
          ._get("connection.delegate").asInstanceOf[Connection];

        new InternalBlobValue(new RemoteBlob(conn, remoteHandle, entry.length, entry.mimeType));

      case BlobIO.BOLT_VALUE_TYPE_BLOB_INLINE =>
        in.readByte();

        val values = for (i <- 0 to 3) yield in.readLong();
        val entry = BlobIO.unpack(values.toArray);

        //read inline
        val length = entry.length;
        val bs = new Array[Byte](length.toInt);
        in.readBytes(bs, 0, length.toInt);
        new InternalBlobValue(new InlineBlob(bs, length, entry.mimeType));

      case _ => null;
    }
  }

  //client side?
  def packBlob(blob: Blob, packer: org.neo4j.driver.internal.packstream.PackStream.Packer): Unit = {
    val out = packer._get("out").asInstanceOf[org.neo4j.driver.internal.packstream.PackOutput];
    //create a temp blodid
    val tempBlobId = BlobId.EMPTY;
    out.writeByte(BlobIO.BOLT_VALUE_TYPE_BLOB_INLINE);

    //write blob entry
    BlobIO._pack(Blob.makeEntry(tempBlobId, blob)).foreach(out.writeLong(_));

    //write inline
    val bytes = blob.toBytes();
    out.writeBytes(bytes);
  }
}