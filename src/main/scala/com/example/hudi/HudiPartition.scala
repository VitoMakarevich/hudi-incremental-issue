package com.example.hudi

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{ceil, col, rand, randn}

object HudiPartition extends App {
  val spark = SparkSession.builder().master("local[*]").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._
  val outputPath = "/tmp/hudi/input"
  val sparkFormatOutputPath = s"file://$outputPath"
  val commonDefaultOptions = Map(
    "hoodie.datasource.write.table.type" -> "COPY_ON_WRITE",
    "hoodie.metadata.enable" -> "false",
    // use slightly modified name instead of _hoodie_commit_time for populating test data from JSON
    // since columns started with "_" aren't allowed in Hudi/Avro names
    "hoodie.datasource.write.precombine.field" -> "id",
    "hoodie.datasource.write.recordkey.field" -> "id",
    "hoodie.datasource.write.keygenerator.class" -> "org.apache.hudi.keygen.ComplexKeyGenerator",
    "hoodie.datasource.write.hive_style_partitioning" -> "true",
    "hoodie.datasource.write.partitionpath.field" -> "partition1,partition2",
    "hoodie.table.name" -> "example",
    "hoodie.finalize.write.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
  )

  for(i <- 1.to(2)) {
    spark.range(100)
      .withColumn("partition1", ceil(rand() * 10))
      .withColumn("partition2", ceil(rand() * 10))
      .write
      .format("hudi")
      .options(commonDefaultOptions)
      .mode(if(i == 1) SaveMode.Overwrite else SaveMode.Append)
      .save(outputPath)
  }

  print("Write")

  val df = spark.read.format("hudi").load(outputPath).filter($"partition1" > 3)
  df.explain(true)
  df.show(false)
}
