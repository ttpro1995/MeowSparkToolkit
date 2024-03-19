package me.thaithien.sparktoolkit.common.parameter

import me.thaithien.sparktoolkit.common.file_format.{FileFormat, Parquet}

sealed trait PathImplicitUnionType[T]

object PathImplicitUnionType {

  implicit object ListStringPathImplicitUnionType extends PathImplicitUnionType[List[String]]

  implicit object StringPathImplicitUnionType extends PathImplicitUnionType[String]

}

case class PathParameter[A: PathImplicitUnionType](protected val value: Option[A],
                                                   mode: FileFormat = Parquet()) extends Parameter[A, List[String]] {
  def getValue: List[String] = value match {
    case Some(v) => v match {
      case v: String => if (v.contains("{")) List(v) else v.split(",").toList
      case v: List[_] => v.map(_.toString)
      case _ => ???
    }
    case _ => ???
  }
}

object PathParameter {
  def apply(mode: FileFormat): PathParameter[String] = PathParameter[String](value = None, mode = mode)
  def apply(): PathParameter[String] = PathParameter[String](value = None, mode = Parquet())
}