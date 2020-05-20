package cn.pandadb.blob

import java.io.ByteArrayOutputStream
import cn.pandadb.util.StreamUtils
import cn.pandadb.util.StreamUtils._

/**
  * Created by bluejoe on 2019/4/18.
  */
object BlobIO {
//  val BOLT_VALUE_TYPE_BLOB_INLINE = 0xC5.toByte;
//  val BOLT_VALUE_TYPE_BLOB_REMOTE = 0xC4.toByte;
  val BLOB_PROPERTY_TYPE = 15;
  val MAX_INLINE_BLOB_BYTES = 10240;

  def pack(entry: BlobEntry): Array[Byte] = {
    val baos = new ByteArrayOutputStream();
    _pack(entry, 0).foreach(baos.writeLong(_));

    baos.toByteArray;
  }

  def unpack(values: Array[Long]): BlobEntry = {
    val length = values(1) >> 16;
    val mimeType = values(1) & 0xFFFFL;

    val bid = new BlobId(values(2), values(3));

    val mt = MimeType.fromCode(mimeType);
    Blob.makeEntry(bid, length, mt);
  }

  def _pack(entry: BlobEntry, keyId: Int = 0): Array[Long] = {
    val values = new Array[Long](4);
    //val digest = ByteArrayUtils.convertByteArray2LongArray(blob.digest);
    /*
    blob uses 4*8 bytes: [v0][v1][v2][v3]
    v0: [____,____][____,____][____,____][____,____][[____,tttt][kkkk,kkkk][kkkk,kkkk][kkkk,kkkk] (t=type, k=keyId)
    v1: [llll,llll][llll,llll][llll,llll][llll,llll][llll,llll][llll,llll][mmmm,mmmm][mmmm,mmmm] (l=length, m=mimeType)
    v2: [iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii]
    v3: [iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii]
    */
    values(0) = keyId | (BLOB_PROPERTY_TYPE << 24);
    values(1) = entry.mimeType.code | (entry.length << 16);
    val la = StreamUtils.convertByteArray2LongArray(entry.id.asByteArray());
    values(2) = la(0);
    values(3) = la(1);

    values;
  }
}