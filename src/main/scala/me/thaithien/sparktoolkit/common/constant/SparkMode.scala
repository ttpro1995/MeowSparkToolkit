package me.thaithien.sparktoolkit.common.constant

/**
 * @author baopng
 */

object SparkMode extends Enumeration {
  type SparkMode = Value
  val LOCAL, YARN, AUTO = Value
}
