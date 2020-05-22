package cn.pandadb.blob.storage.impl

import java.io.{File, FileInputStream, FileOutputStream, InputStream}
import java.util.UUID

import cn.pandadb.blob.storage.BlobStorageService
import cn.pandadb.blob.{Blob, BlobEntry, BlobId, InputStreamSource, MimeType}
import cn.pandadb.configuration.Config
import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.commons.io.filefilter.TrueFileFilter
import cn.pandadb.util.StreamUtils._

import scala.tools.nsc.interpreter.InputStream

class LocalFileSystemBlobValueStorage(config: Config) extends BlobStorageService{
  val logger = config.getLogger(this.getClass)
  val rootDir: String = "/blob";

  override def save(length: Long, mimeType: MimeType, inputStream: InputStream): BlobEntry = {
      val bid = generateId();
//      val file = locateFile(bid);
//      file.getParentFile.mkdirs();
//
//      val fos = new FileOutputStream(file);
//      fos.write(bid.asByteArray());
//      fos.writeLong(blob.mimeType.code);
//      fos.writeLong(blob.length);
//
//      blob.offerStream { bis =>
//        IOUtils.copy(bis, fos);
//      }
//      fos.close();
    logger.info(this.getClass + ": save" + "| " + bid.asLiteralString())

      Blob.makeEntry(bid, length, mimeType)
  }

  override def load(id: BlobId): Option[Blob] = {
    val file = locateFile(id)
    Option(readFromBlobFile(file))
  }

  override def delete(id: BlobId): Unit = {
    val file = locateFile(id)
    file.delete()
  }

  private def generateId(): BlobId = {
    val uuid = UUID.randomUUID()
    BlobId(uuid.getMostSignificantBits, uuid.getLeastSignificantBits)
  }

  private def locateFile(bid: BlobId): File = {
    val idname = bid.asLiteralString()
    new File(rootDir, s"${idname.substring(28, 32)}/$idname")
  }

  private def readFromBlobFile(blobFile: File): Blob = {
    val fis = new FileInputStream(blobFile)
    val blobId = BlobId.readFromStream(fis)
    val mimeType = MimeType.fromCode(fis.readLong())
    val length = fis.readLong()
    fis.close()

    Blob.fromInputStreamSource(blobId, new InputStreamSource() {
      def offerStream[T](consume: (InputStream) => T): T = {
        val is = new FileInputStream(blobFile)
        //NOTE: skip
        is.skip(8 * 4)
        val t = consume(is)
        is.close()
        t
      }
    }, length, Some(mimeType))
  }

  override def start(): Unit = {
    val rootDirFile = new File(rootDir)
    if (!rootDirFile.exists()) {
      rootDirFile.mkdirs()
    }
    logger.info(s"using storage dir: ${rootDirFile.getCanonicalPath}");
  }

  override def stop(): Unit = {
    logger.info(this.getClass + ": stop")
  }

}
