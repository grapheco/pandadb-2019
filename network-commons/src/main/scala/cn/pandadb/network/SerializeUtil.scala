package cn.pandadb.network

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created at 11:50 2019/12/4
  * @Modified By:
  */
object BytesTransform {

  def serialize[T](o: T): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(o)
    oos.close()
    bos.toByteArray
  }

  def deserialize[T](bytes: Array[Byte]): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)
    ois.readObject.asInstanceOf[T]
  }
}
