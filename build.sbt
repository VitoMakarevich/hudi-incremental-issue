name := "hudi-incremental-issue"

version := "0.1"

scalaVersion := "2.12.13"

val Spark = "3.3.0"
val Hudi = "0.12.2"
val Logging = "3.9.4"

libraryDependencies ++= Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % Logging,
  "org.apache.hudi" %% "hudi-spark3.3-bundle" % Hudi,
  "org.apache.spark" %% "spark-sql" % Spark,
  "org.apache.spark" %% "spark-core" % Spark,
  "org.apache.spark" %% "spark-hive" % Spark,
  "org.typelevel" %% "cats-core" % "2.10.0"
)

scalacOptions += "-Ypartial-unification"

Compile / run / mainClass := Some("com.example.hudi.HudiIncrementalChecker")