package com.example.hudi

import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}

object HudiRefreshBug extends App {

  case class Input(key: String, partition1: Int, value: String, precombine: Int)

  val spark = SparkSession
    .builder()
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val outputPath = "/tmp/hudi/input"
  val sparkFormatOutputPath = s"file://$outputPath"
  val commonDefaultOptions = Map(
    "hoodie.datasource.write.table.type" -> "COPY_ON_WRITE",
    "hoodie.metadata.enable" -> "true",
    "hoodie.datasource.write.operation" -> "insert",
    "hoodie.datasource.write.precombine.field" -> "precombine",
    "hoodie.datasource.write.recordkey.field" -> "key",
    "hoodie.datasource.write.keygenerator.class" -> "org.apache.hudi.keygen.ComplexKeyGenerator",
    "hoodie.datasource.write.hive_style_partitioning" -> "true",
    "hoodie.datasource.write.partitionpath.field" -> "partition1",
    "hoodie.table.name" -> "example",
    "hoodie.finalize.write.parallelism" -> "2",
    "hoodie.upsert.shuffle.parallelism" -> "2",
    "hoodie.datasource.write.drop.partition.columns" -> "true"
  )

  val firstPartition = 1
  val inputFirst = List(Input("1", firstPartition, "1", 1))

  inputFirst.toDF().write
    .format("hudi")
    .options(commonDefaultOptions)
    .mode(SaveMode.Overwrite)
    .save(outputPath)

  spark.read.schema(Encoders.product[Input].schema).format("hudi").load(outputPath).createTempView("hudi_table")
  val contentFirstRead = spark.table("hudi_table").as[Input].collect()
  assert(contentFirstRead.length == 1)
  assert(contentFirstRead(0) == inputFirst(0))

  val secondPartition = 2
  val inputSecond = List(Input("10", secondPartition, "2", 200), Input("20", firstPartition, "2", 200))
  inputSecond.toDF().write
    .format("hudi")
    .options(commonDefaultOptions)
    .mode(SaveMode.Append)
    .save(outputPath)

  // Refresh view - here I expect to now see 3 rows in 2 partitions as in Hudi 0.12.x
  spark.catalog.refreshTable("hudi_table")

  val contentSecondRead = spark.table("hudi_table").as[Input].collect()

  // Expected - all 3 rows created, in Hudi 0.12.1 it works, in 0.13.1 - fails, only 2 rows exist.
  // Problem in Hudi file index
  assert(contentSecondRead.length == 3)

  print("Write end")
}
