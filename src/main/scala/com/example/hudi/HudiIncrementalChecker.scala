package com.example.hudi

import com.typesafe.scalalogging.LazyLogging
import org.apache.hudi.DataSourceReadOptions.{BEGIN_INSTANTTIME, QUERY_TYPE, QUERY_TYPE_INCREMENTAL_OPT_VAL}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File

object HudiIncrementalChecker extends LazyLogging {
  val inputs = Seq(
    Seq(
      ("1", "1", "1")
    ),
    Seq(
      ("2", "2", "2")
    ),
    Seq(
      ("3", "3", "3")
    ),
    Seq(
      ("4", "4", "4")
    ),
    Seq(
      ("5", "5", "5"),
    ),
  )

  val commonDefaultOptions = Map(
    "hoodie.datasource.write.table.type" -> "COPY_ON_WRITE",
    // use slightly modified name instead of _hoodie_commit_time for populating test data from JSON
    // since columns started with "_" aren't allowed in Hudi/Avro names
    "hoodie.datasource.write.precombine.field" -> "precombine",
    "hoodie.datasource.write.recordkey.field" -> "id",
    "hoodie.datasource.write.keygenerator.class" -> "org.apache.hudi.keygen.ComplexKeyGenerator",
    "hoodie.datasource.write.hive_style_partitioning" -> "true",
    "hoodie.datasource.write.partitionpath.field" -> "",
    "hoodie.table.name" -> "example",
    "hoodie.finalize.write.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
  )
  // By default, hudi could handle 5 commits without cleaning
  // With that options it will retain 3 commits
  val withCompactionOptions = commonDefaultOptions ++ Map(
    "hoodie.keep.max.commits" -> "3",
    "hoodie.keep.min.commits" -> "2",
    "hoodie.cleaner.commits.retained" -> "1"
  )
  val outputPath = "/tmp/hudi/input"
  val sparkFormatOutputPath = s"file://$outputPath"
  val nonTrimmedOutputPath = s"${sparkFormatOutputPath}/non-trimmed"
  val trimmedOutputPath = s"${sparkFormatOutputPath}/trimmed"

  def main(args: Array[String]): Unit = {
    import org.apache.commons.io.FileUtils;

    FileUtils.deleteDirectory(new File(outputPath));
    val spark = SparkSession.builder().master("local[*]").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate()

    val nonTrimmed = runIngestionAndReturnAllUpdatesIncremental(spark, commonDefaultOptions, nonTrimmedOutputPath)
    val countNonTrimmed = nonTrimmed.count()
    logger.info(s"Count of rows received incrementally without clean ${countNonTrimmed}")
    val trimmed = runIngestionAndReturnAllUpdatesIncremental(spark, withCompactionOptions, trimmedOutputPath)
    val countTrimmed = trimmed.count()
    // Here hudi 0.9.0 will return 3 rows, hudi 0.10.0 will return 4
    logger.info(s"Count of rows received incrementally with clean ${countTrimmed}")
    val snapshotTrimmed = snapshot(spark, trimmedOutputPath)
    logger.info(s"Count of rows in snapshot for trimmed output ${snapshotTrimmed.count()}")
    val snapshotNonTrimmed = snapshot(spark, nonTrimmedOutputPath)
    logger.info(s"Count of rows in snapshot for non trimmed output ${snapshotNonTrimmed.count()}")
  }

  def runIngestionAndReturnAllUpdatesIncremental(spark: SparkSession, options: Map[String, String], output: String): DataFrame = {
    import spark.implicits._
    for(input <- inputs) {
      val df = spark.sparkContext.parallelize(input).toDF("id", "precombine", "value")
      df.write.format("hudi").options(options).mode("append").save(output)
    }

    val incremental = spark.read.format("hudi").options(Map(
      QUERY_TYPE.key() -> QUERY_TYPE_INCREMENTAL_OPT_VAL,
      BEGIN_INSTANTTIME.key() -> "0"
    )).load(output)

    incremental
  }
  def snapshot(sparkSession: SparkSession, output: String): DataFrame = {
    val snapshot = sparkSession.read.format("hudi").options(Map(
      QUERY_TYPE.key() -> "snapshot",
    )).load(output)

    snapshot
  }
}
