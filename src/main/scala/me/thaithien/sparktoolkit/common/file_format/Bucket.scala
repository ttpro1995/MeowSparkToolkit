package me.thaithien.sparktoolkit.common.file_format

import scala.util.Random

/**
 * @author baopng
 */

case class Bucket(table_name: String = s"table_${Random.nextInt(Int.MaxValue)}",
                  num_buckets: Int = 200,
                  bucket_cols: Seq[String] = Seq("src_id"),
                  schema: String = "",
                  repartition_before_write: Boolean = true,
                  sort_cols: Seq[String] = Seq(),
                  check_num_buckets: Boolean = true) extends FileFormat
