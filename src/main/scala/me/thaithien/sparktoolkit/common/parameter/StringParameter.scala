package me.thaithien.sparktoolkit.common.parameter

/**
 * @author baopng
 */

case class StringParameter(protected val value: Option[String] = None) extends Parameter[String, String] {
  def getValue: String = value match {
    case Some(v) => v
    case _ => ???
  }
}
