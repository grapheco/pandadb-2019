package cn.pandadb.neo4j.driver.values

import java.util.Objects

trait Point extends Serializable {
  def srid: Int

  def x: Double

  def y: Double

  def z: Double
}

case class Point2D(ss: Int, xx: Double, yy: Double) extends Point with Serializable {
  override def srid: Int = ss

  override def x: Double = xx

  override def y: Double = yy

  override def z: Double = Double.NaN

  //  override def equals(o: Any): Boolean = {
  //    if (this == o)
  //      return true
  //    if (o == null || (getClass ne o.getClass))
  //      return false
  //    val that = o.asInstanceOf[Point2D]
  //    srid == that.srid && that.x ==  x && that.y == y
  //  }

  override def toString: String = s"point({srid:$srid, x:$x, y:$y})"
}

case class Point3D(ss: Int, xx: Double, yy: Double, zz: Double) extends Point with Serializable {
  override def srid: Int = ss

  override def x: Double = xx

  override def y: Double = yy

  override def z: Double = zz

  //  override def equals(o: Any): Boolean = {
  //    if (this == o)
  //      return true
  //    if (o == null || (getClass ne o.getClass))
  //      return false
  //    val that = o.asInstanceOf[Point3D]
  //    srid == that.srid && that.x ==  x && that.y == y && that.z == z
  //  }

  override def toString: String = s"point({srid:$srid, x:$x, y:$y, z:$z})"
}

