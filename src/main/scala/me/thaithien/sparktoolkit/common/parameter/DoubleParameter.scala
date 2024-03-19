package me.thaithien.sparktoolkit.common.parameter

/**
 * @author baopng
 */

sealed trait DoubleImplicitUnionType[T]

object DoubleImplicitUnionType {

  implicit object DoubleDoubleImplicitUnionType extends DoubleImplicitUnionType[Double]

  implicit object FloatDoubleImplicitUnionType extends DoubleImplicitUnionType[Float]

  implicit object StringDoubleImplicitUnionType extends DoubleImplicitUnionType[String]

}

case class DoubleParameter[T: DoubleImplicitUnionType](protected val value: Option[T] = None) extends Parameter[T, Double] {
  def getValue: Double = value match {
    case Some(v) => v match {
      case v: Double => v
      case v: Float => v.toDouble
      case v: String => v.toDouble
      case _ => ???
    }
    case _ => ???
  }
}
