package com.example.hudi

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{ceil, col, expr, lit, rand}

object HudiClusteringPartitionColumn extends App {
  val spark = SparkSession
    .builder()
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
    //        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
    //        .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val outputPath = "/tmp/hudi/input"
  val sparkFormatOutputPath = s"file://$outputPath"
  val commonDefaultOptions = Map(
    "hoodie.datasource.write.table.type" -> "COPY_ON_WRITE",
    "hoodie.metadata.enable" -> "false",
    "hoodie.datasource.write.operation" -> "upsert",
    "hoodie.datasource.write.precombine.field" -> "key",
    "hoodie.datasource.write.recordkey.field" -> "key",
    "hoodie.datasource.write.keygenerator.class" -> "org.apache.hudi.keygen.ComplexKeyGenerator",
    "hoodie.datasource.write.hive_style_partitioning" -> "true",
    "hoodie.datasource.write.partitionpath.field" -> "partition1",
    "hoodie.table.name" -> "example",
    "hoodie.finalize.write.parallelism" -> "10",
    "hoodie.upsert.shuffle.parallelism" -> "10",
    "hoodie.datasource.write.drop.partition.columns" -> "true",
    "hoodie.clustering.inline" -> "true",
    "hoodie.clustering.inline.max.commits" -> "5",
    "hoodie.clustering.plan.strategy.sort.columns" -> "value"
  )
  def write() = {

    for (i <- 1.to(10)) {
      spark.range(10)
        .drop("id")
        .withColumn("value", expr("uuid()"))
        .withColumn("key", expr("uuid()"))
        .withColumn("partition1", ceil(rand() * 3))
        .write
        .format("hudi")
        .options(commonDefaultOptions)
        .mode(if (i == 1) SaveMode.Overwrite else SaveMode.Append)
        .save(outputPath)
      println(s"Written ${i} batch")
    }

    print("Write end")
  }

  write()
}
