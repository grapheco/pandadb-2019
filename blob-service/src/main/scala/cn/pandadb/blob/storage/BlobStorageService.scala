package cn.pandadb.blob.storage

import cn.pandadb.blob.{Blob, BlobEntry, BlobId, MimeType}
import cn.pandadb.server.modules.LifecycleServerModule

import scala.tools.nsc.interpreter.InputStream

trait BlobStorageService extends LifecycleServerModule {
  def save(length: Long, mimeType: MimeType, inputStream: InputStream): BlobEntry;

  def load(id: BlobId): Option[Blob];

  def delete(id: BlobId): Unit;
}
