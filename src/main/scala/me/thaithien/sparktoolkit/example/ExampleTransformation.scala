package me.thaithien.sparktoolkit.example

import me.thaithien.sparktoolkit.common.file_format.{Bucket, FileFormat}
import me.thaithien.sparktoolkit.common.parameter.{PathParameter, StringParameter}
import me.thaithien.sparktoolkit.common.spark.SparkBase
import org.apache.spark.sql.DataFrame

object ExampleTransformation extends SparkBase {

  // use it like a dataframe
  val inputDataFrame: PathParameter[String] = PathParameter(mode = Bucket(num_buckets = 100, bucket_cols = Seq("meow_id")))

  // use .getValue to get as String
  val someStringParam: StringParameter = StringParameter()

  override val write_mode: FileFormat = Bucket(num_buckets = 100, bucket_cols = Seq("meow_id"))

  override def output: DataFrame = {
    val colName: String = someStringParam.getValue
    val outputDF: DataFrame = inputDataFrame.select(colName)
    return outputDF
  }
}
