package cn.pandadb.blob.storage.impl

import java.util.UUID
import java.io.{ByteArrayOutputStream, InputStream}
import java.nio.ByteBuffer

import cn.pandadb.blob.{Blob, BlobEntry, BlobId, InputStreamSource, MimeType}
import cn.pandadb.blob.storage.BlobStorageService
import cn.pandadb.configuration.Config
import cn.pandadb.util.StreamUtils._
import org.grapheco.regionfs.client.FsClient
import org.apache.commons.io.IOUtils
import org.grapheco.regionfs.FileId

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class RegionfsBlobValueStorage(config: Config) extends BlobStorageService {
  val logger = config.getLogger(this.getClass)
  val fsZKAddress = config.getRegionfsZkAddress()

  val fsClient = new FsClient(fsZKAddress)

  override def save(length: Long, mimeType: MimeType, inputStream: InputStream): BlobEntry = {
    save(length, mimeType, IOUtils.toByteArray(inputStream))
  }

  override def save(length: Long, mimeType: MimeType, bytes: Array[Byte]): BlobEntry = {
    val baos = new ByteArrayOutputStream()
    baos.writeLong(length)
    baos.writeLong(mimeType.code)
    baos.write(bytes)

    val future = fsClient.writeFile(ByteBuffer.wrap(baos.toByteArray))
    val fileId: FileId = Await.result(future, Duration.Inf)
    val bid = fileId2BlobId(fileId)
    logger.info(this.getClass + ": save" + "| " + bid.asLiteralString())

    Blob.makeEntry(bid, length, mimeType)
  }

  override def load(id: BlobId): Option[Blob] = {
    val future = fsClient.readFile(blobId2FileId(id), ins => {
      val length = ins.readLong()
      val mimeType = MimeType.fromCode(ins.readLong())
      Blob.fromInputStreamSource(id, new InputStreamSource {
        override def offerStream[T](consume: InputStream => T): T = {
          val t = consume(ins)
          ins.close()
          t
        }
      }, length, Some(mimeType))
    })
    val blob = Await.result(future, Duration.Inf)
    logger.info("load blob: " + blob.toString)
    Some(blob)
  }

  override def delete(id: BlobId): Unit = {
    fsClient.deleteFile(blobId2FileId(id)).wait()
    logger.info("delete blob: " + id.asLiteralString())
  }

  private def fileId2BlobId(id: FileId): BlobId = {
    BlobId(id.regionId, id.localId)
  }

  private def blobId2FileId(id: BlobId): FileId = {
    FileId(id.value1, id.value2)
  }

}
