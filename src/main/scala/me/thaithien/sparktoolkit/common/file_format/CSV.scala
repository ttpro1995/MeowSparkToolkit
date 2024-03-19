package me.thaithien.sparktoolkit.common.file_format

import org.apache.spark.sql.types.StructType

/**
 * @author baopng
 */

case class CSV(header: Boolean = true, sep: String = "\t", schema: Option[StructType] = None) extends FileFormat
