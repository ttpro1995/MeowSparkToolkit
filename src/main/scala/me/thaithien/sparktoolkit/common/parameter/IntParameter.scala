package me.thaithien.sparktoolkit.common.parameter

/**
 * @author baopng
 */

sealed trait IntImplicitUnionType[T]

object IntImplicitUnionType {

  implicit object IntIntImplicitUnionType extends IntImplicitUnionType[Int]

  implicit object StringIntImplicitUnionType extends IntImplicitUnionType[String]

}

case class IntParameter[A: IntImplicitUnionType](protected val value: Option[A] = None) extends Parameter[A, Int] {
  def getValue: Int = value match {
    case Some(v) => v match {
      case v: Int => v
      case v: String => v.toInt
      case _ => ???
    }
    case _ => ???
  }
}

object IntParameter {
  def apply(): IntParameter[String] = IntParameter[String](value = None)
}
