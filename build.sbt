name := "hudi-incremental-issue"

version := "0.1"

scalaVersion := "2.12.11"

val Spark = "3.3.0"
val Hudi = "0.12.1"
val Logging = "3.9.4"

libraryDependencies ++= Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % Logging,
  "org.apache.hudi" %% "hudi-spark3-bundle" % Hudi,
  "org.apache.spark" %% "spark-sql" % Spark,
  "org.apache.spark" %% "spark-core" % Spark,
  "org.apache.spark" %% "spark-hive" % Spark
)

Compile / run / mainClass := Some("com.example.hudi.HudiIncrementalChecker")