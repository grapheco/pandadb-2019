package cn.pandadb.blob.util

import java.io.InputStream

import cn.pandadb.blob.InputStreamSource
import org.apache.commons.codec.binary.Hex
import org.apache.commons.codec.digest.DigestUtils
import cn.pandadb.util.StreamUtils

/**
  * Created by bluejoe on 2018/8/9.
  */
object CodecUtils {
  val md5: DigestUtils = new DigestUtils(DigestUtils.getMd5Digest)

  /**
    * create 128-bits(16bytes) digest
    *
    * @param is
    * @return
    */
  def md5(is: InputStream): Array[Byte] =
    md5.digest(is);

  def md5(iss: InputStreamSource): Array[Byte] = {
     iss.offerStream { is =>
     md5(is);
   }
  }

  def md5AsHex(is: InputStream): String =
    Hex.encodeHexString(md5(is));

  def md5AsHex(bytes: Array[Byte]): String =
    md5.digestAsHex(bytes);

  def encodeHexString(bytes: Array[Byte]): String =
    Hex.encodeHexString(bytes);

  def md5AsHex(value: Long): String =
    md5AsHex(StreamUtils.convertLongArray2ByteArray(Array(value)));
}
