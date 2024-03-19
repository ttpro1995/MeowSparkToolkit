package me.thaithien.sparktoolkit.common.utils

object Converter {
  def toTuple2[T]: Seq[T] => (T, T) = {
    case Seq(a, b, _*) => (a, b)
  }
}
