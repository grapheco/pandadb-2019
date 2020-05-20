package cn.pandadb.blob.storage

import cn.pandadb.blob.{Blob, BlobId}
import cn.pandadb.server.modules.LifecycleServerModule

trait BlobStorageService extends LifecycleServerModule {
  def save(blob: Blob): BlobId;

  def load(id: BlobId): Option[Blob];

  def delete(id: BlobId): Unit;
}
