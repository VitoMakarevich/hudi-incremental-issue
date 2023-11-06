package com.example.hudi

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{ceil, col, rand, expr}

object HudiEmbedMetadataTest extends App {
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
    "hoodie.datasource.write.precombine.field" -> "key",
    "hoodie.datasource.write.recordkey.field" -> "key",
    "hoodie.datasource.write.keygenerator.class" -> "org.apache.hudi.keygen.ComplexKeyGenerator",
    "hoodie.datasource.write.hive_style_partitioning" -> "true",
    "hoodie.datasource.write.partitionpath.field" -> "partition1,partition2,partition3",
    "hoodie.table.name" -> "example",
    "hoodie.finalize.write.parallelism" -> "100",
    "hoodie.upsert.shuffle.parallelism" -> "100",
    "hoodie.datasource.write.drop.partition.columns" -> "true"
  )

  for(i <- 1.to(15)) {
    spark.range(1000)
      .drop("id")
      .withColumn("key", expr("uuid()"))
      .withColumn("partition1", ceil(rand() * 100))
      .withColumn("partition2", ceil(rand() * 100))
      .withColumn("partition3", ceil(rand() * 100))
      .write
      .format("hudi")
      .options(commonDefaultOptions)
      .mode(SaveMode.Append)
      .save(outputPath)
    print(s"Written ${i} batch")
  }

  print("Write end")
}
