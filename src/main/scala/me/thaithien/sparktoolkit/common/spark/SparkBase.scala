package me.thaithien.sparktoolkit.common.spark

import me.thaithien.sparktoolkit.common.constant.SparkMode
import me.thaithien.sparktoolkit.common.exception.ParameterException.{UnassignedParamException, UnknownParamException}
import me.thaithien.sparktoolkit.common.exception.ReadingException.WrongInputFormatException
import me.thaithien.sparktoolkit.common.exception.WritingException.{UnimplementedWritingCustomFormatException, WrongOutputFormatException}
import me.thaithien.sparktoolkit.common.file_format._
import me.thaithien.sparktoolkit.common.logger.OffLogger
import me.thaithien.sparktoolkit.common.parameter.{ArgumentsParser, Parameter, PathParameter, StringParameter}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/** Provides basic tools to construct a spark task.
 *
 * Includes methods to declare path parameters and to switch between yarn and local running mode conveniently.
 *
 * @author baopng
 * @author thanhnm3
 */

trait SparkBase extends OffLogger {

  /** The configuration in which spark session is created
   *
   * SparkMode, default=AUTO
   * if AUTO, then set to YARN if there is any parameter in the running command, LOCAL otherwise.
   */
  val spark_mode: SparkMode.Value = SparkMode.AUTO

  def set_spark_mode(in_local: Boolean): Unit = {
    if (spark_mode == SparkMode.AUTO) {
      this.getClass.getDeclaredFields.filter(_.getName == "spark_mode").foreach(field => {
        field.setAccessible(true)
        field.set(this, if (in_local) SparkMode.LOCAL else SparkMode.YARN)
      })
    }
  }

  /** Sets values for spark.sql.shuffle.partitions
   *
   * Int, default=200
   */
  val shuffle_partitions = 200

  def set_shuffle_partitions(): Unit = {
    if (shuffle_partitions != 200) {
      spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions)
    }
  }

  /** Creates spark session.
   *
   * This attribute MUST stay lazy. Default = run in local.
   * set_spark() uses reflector to change this config to run in yarn.
   */
  lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  def set_spark(): Unit = {
    this.getClass.getDeclaredFields.filter(_.getName == "spark").foreach(field => {
      field.setAccessible(true)
      field.set(this, SparkSession.builder().getOrCreate())
    })
  }

  /** Reassigns run-time value of parameters. Mostly effective in YARN spark_mode.
   *
   * @param kargs a map of parameter names to their values in string format
   */
  def set_attr(kargs: Map[String, String]): Unit = {
    val fields = this.getClass.getDeclaredFields
    val param_dict = fields.map(field => {
      field.setAccessible(true)
      (field.getName, field.get(this))
    })
      .filter(_._2.isInstanceOf[Parameter[_, _]])
      .map { case (k, v) => v match {
        case v: Parameter[_, _] => k -> v
      }
      }
    val unknown_params = kargs.keys.filterNot(x => param_dict.exists(_._1 == x))
    if (unknown_params.nonEmpty) {
      throw UnknownParamException(unknown_params)
    }
    val parsed_param_dict = param_dict.map {
      case (k, v) =>
        if (kargs.contains(k)) (k, v.modify(kargs(k)))
        else if (v.isEmpty) throw UnassignedParamException(k)
        else (k, v)
    }
    fields.foreach(field => {
      if (parsed_param_dict.exists(_._1 == field.getName))
        field.set(this, parsed_param_dict.find(_._1 == field.getName).get._2)
    })
  }

    /** READING: Converts PathParameter to DataFrame implicitly.
     *
     * @param input_paths a PathParameter object with arbitrary mode
     * @return a DataFrame returned by a corresponding "read" function
     */
    implicit def read(input_paths: PathParameter[_]): DataFrame = input_paths.mode match {
      case _: Parquet => read_parquet(input_paths)
      case _: CSV => read_csv(input_paths)
      case _: Bucket => if (spark_mode == SparkMode.LOCAL) read_parquet(input_paths) else read_bucket(input_paths)
      case _: Custom => read_custom(input_paths)
      case _ => throw WrongInputFormatException()
    }

    //Parquet. Uses unionByName to avoid mismatch schemas
    def read_parquet(input_paths: PathParameter[_]): DataFrame = {
      input_paths.getValue.map(spark.read.parquet).reduce(_ unionByName _)
    }

    //CSV.
    def read_csv(input_paths: PathParameter[_]): DataFrame = {
      val mode = input_paths.mode.asInstanceOf[CSV]
      mode.schema match {
        case Some(s) => spark.read.schema(s).option("header", mode.header).option("sep", mode.sep).csv(input_paths.getValue: _*)
        case _ => spark.read.option("header", mode.header).option("sep", mode.sep).csv(input_paths.getValue: _*)
      }
    }

  //Bucket.
  def read_bucket(input_paths: PathParameter[_]): DataFrame = {
    def getFilePaths(inputPath: String) = {
      val fs = FileSystem.get(
        new java.net.URI(inputPath.replaceAll("[?|^|\\[|\\]|\\{|\\}]", "")), spark.sparkContext.hadoopConfiguration
      )

      val fileStatus = fs.globStatus(new Path(inputPath))

      val deepestFolders = (
        if (fileStatus.exists(_.isDirectory)) fileStatus else
          fs.globStatus(new Path(inputPath.split("/").init.mkString("/")))
        ).map(_.getPath)

      val folder = deepestFolders.find(fs.listStatus(_).map(_.getPath).count(!_.getName.startsWith("_")) > 0)

      val filePaths = folder match {
        case None => throw new Exception(f"Path ${inputPath} is empty.")
        case Some(p: Path) => {
          fs.listStatus(p).map(_.getPath).filter(!_.getName.startsWith("_")).map(_.toString)
        }
      }

      filePaths
    }

    val mode = input_paths.mode.asInstanceOf[Bucket]
    val input_path = input_paths.getValue.head
    // throw Exception if there are more than one path
    if (input_paths.getValue.length > 1) ???
    val schema_ddl = if (mode.schema.isEmpty | mode.check_num_buckets) {
      val filePaths = getFilePaths(input_path)
      if (mode.check_num_buckets) {
        val bucketPattern = """_\d*\.c""".r
        val numDiscoveredBuckets = filePaths.map(bucketPattern.findFirstIn(_)).filter(_.nonEmpty).distinct.length
        assert(
          numDiscoveredBuckets == mode.num_buckets,
          f"Number of specified buckets [${mode.num_buckets}] doesn't match with number of "
            + f"discovered buckets [${numDiscoveredBuckets}]. File path [${input_path}]"
        )
      }
      if (mode.schema.isEmpty) spark.read.parquet(filePaths.head).schema.toDDL else mode.schema
    } else mode.schema

    spark.sql(s"DROP TABLE IF EXISTS ${mode.table_name}")
    spark.sql(
      s"""
        CREATE TABLE ${mode.table_name}($schema_ddl)
        USING PARQUET
        CLUSTERED BY (${mode.bucket_cols.mkString(", ")})
        SORTED BY (${mode.bucket_cols.mkString(", ")}) INTO ${mode.num_buckets} BUCKETS
        LOCATION '${input_path}'
        """)
    spark.table(s"${mode.table_name}")
  }

    //Custom. This function merely exists for the sake of generalization. Hope it prove to be useful one day.
    def read_custom(input_paths: PathParameter[_]): DataFrame = ???

    implicit def option_wrapper[A](a: A): Option[A] = Some(a)

    /** WRITING: Handles output DataFrame.
     *
     * Includes functions for showing output in local and writing output in yarn
     */
    def write_output_local(output_df: DataFrame): Unit = {
      output_df.show()
    }

    def write_output_yarn(output_df: DataFrame): Unit = {
      val save_mode = if (overwrite) SaveMode.Overwrite else SaveMode.ErrorIfExists
      val path = output_path.getValue
      write_mode match {
        case _: Parquet => write_parquet(output_df, save_mode, path)
        case params: CSV => write_csv(output_df, save_mode, path, params)
        case params: Bucket => write_bucket(output_df, save_mode, path, params)
        case params: Custom => write_custom(output_df, save_mode, path, params)
        case _ => throw WrongOutputFormatException()
      }
    }

    //Parquet.
    def write_parquet(output_df: DataFrame, save_mode: SaveMode, path: String): Unit = {
      output_df.write.mode(save_mode).parquet(path)
    }

    //CSV.
    def write_csv(output_df: DataFrame, save_mode: SaveMode, path: String, params: CSV): Unit = {
      output_df.write.mode(save_mode).option("header", params.header).option("sep", params.sep).csv(path)
    }

  //Bucket.
  def write_bucket(output_df: DataFrame, save_mode: SaveMode, path: String, params: Bucket): Unit = {
    if (save_mode != SaveMode.Overwrite) {
      val fs = FileSystem.get(new java.net.URI(path), spark.sparkContext.hadoopConfiguration)
      if (fs.exists(new Path(path))) {
        throw new Exception(f"URI: ${fs.getUri.toString}. Path ${path} already exists.")
      }
    }

    (if (params.repartition_before_write) output_df.repartition(params.num_buckets, params.bucket_cols.map(col): _*)
    else output_df)
      .write
      .mode(save_mode)
      .format("parquet")
      .bucketBy(params.num_buckets, params.bucket_cols.head, params.bucket_cols.tail: _*)
      .sortBy(params.bucket_cols.head, params.bucket_cols.tail ++ params.sort_cols: _*)
      .option("path", path)
      .saveAsTable(params.table_name)
  }

    //Custom. Just like its reading counterpart.
    def write_custom(output_df: DataFrame, save_mode: SaveMode, path: String, params: Custom): Unit = {
      throw UnimplementedWritingCustomFormatException()
    }

    //PathParameter for output
    val output_path: StringParameter = StringParameter()

    //Set to true if need to write output in overwrite mode. Default = false
    var overwrite = false

    //Format in which output is written. Default = Parquet.
    val write_mode: FileFormat = Parquet()

    //Defines output DataFrame. To be overridden.
    def output: DataFrame

    /** Main function. Provides a pipeline from reading to writing data. */
    def main(args: Array[String]): Unit = {
      //parses arguments
      val kargs = ArgumentsParser.parse(args)

      set_spark_mode(kargs.isEmpty)

      spark_mode match {
        case SparkMode.LOCAL =>
          write_output_local(output)
        case SparkMode.YARN =>
          set_spark()
          set_attr(kargs)
          set_shuffle_partitions()
          write_output_yarn(output)
        case _ => ???
      }
      spark.stop()
    }
  }
