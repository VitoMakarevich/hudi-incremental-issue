package com.example.hudi

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object HudiAddInnerFieldTest extends LazyLogging {
  val initSchema = StructType(
    Array(
      StructField("key", StructType(Array(
        StructField("id", StringType, false),
      )), false),
      StructField("value", StructType(Array(
        StructField("before", StructType(Array(
          StructField("id", StringType, false),
          StructField("order", StringType, false)
        )), true),
        StructField("after", StructType(Array(
          StructField("id", StringType, false),
          StructField("order", StringType, false)
        )), true),
        StructField("ts_ms", LongType, false),
      )), false),
      StructField("upsert_root_account_id", LongType, false),
    )
  )
  val newSchema = StructType(
    Array(
      StructField("key", StructType(Array(
        StructField("id", StringType, false),
      )), false),
      StructField("value", StructType(Array(
        StructField("before", StructType(Array(
          StructField("id", StringType, false),
          StructField("order", StringType, false),
          StructField("name", StringType, true),
          StructField("name2", StringType, true),
        )), true),
        StructField("after", StructType(Array(
          StructField("id", StringType, false),
          StructField("order", StringType, false),
          StructField("name", StringType, true),
          StructField("name2", StringType, true),
        )), true),
        StructField("ts_ms", LongType, false),
      )), false),
      StructField("upsert_root_account_id", LongType, false),
    )
  )

  val spark = SparkSession.builder().master("local[*]").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate()
  val outputPath = "/tmp/hudi/input"
  val sparkFormatOutputPath = s"file://$outputPath"
  val commonDefaultOptions = Map(
    "hoodie.datasource.write.table.type" -> "MERGE_ON_READ",
    // use slightly modified name instead of _hoodie_commit_time for populating test data from JSON
    // since columns started with "_" aren't allowed in Hudi/Avro names
    "hoodie.datasource.write.precombine.field" -> "value.after.order",
    "hoodie.datasource.write.recordkey.field" -> "key",
    "hoodie.datasource.write.keygenerator.class" -> "org.apache.hudi.keygen.ComplexKeyGenerator",
    "hoodie.datasource.write.hive_style_partitioning" -> "true",
    "hoodie.datasource.write.partitionpath.field" -> "",
    "hoodie.table.name" -> "example",
    "hoodie.finalize.write.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
  )

  def main(args: Array[String]): Unit = {
    import scala.collection.JavaConversions._
    val initData = Seq(
      Row(
        Row("1"),
        Row(Row("1", "1"),Row("1", "1"), 12L),
        23L
      ),
      Row(
        Row("2"),
        Row(Row("2", "2"),Row("2", "2"), 12L),
        23L
      )
    )
    val initDf = spark.createDataFrame(initData, initSchema)
    initDf.write.options(commonDefaultOptions).format("hudi").mode("overwrite").save(sparkFormatOutputPath)

    val dfInputWithColumn = Seq(
      Row(
        Row("1"),
        Row(Row("13", "13", "upd", "upd1"),Row("13", "13", "upd", "upd1"), 12L),
        23L
      ),
      Row(
        Row("3"),
        Row(Row("2", "2", "new", "new1"),Row("2", "2", "new", "new1"), 12L),
        23L
      )
    )
    val dfWithColumn = spark.createDataFrame(dfInputWithColumn, newSchema)
    dfWithColumn.write.options(commonDefaultOptions).format("hudi").mode("append").save(sparkFormatOutputPath)

    val readDF = spark.read.format("hudi").load(sparkFormatOutputPath)
    assert(readDF.count() == 3, "The count of rows doesn't match")
  }
}
