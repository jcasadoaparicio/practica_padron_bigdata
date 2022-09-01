package jca

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

trait spark {
  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkScalaExamples")
    .getOrCreate()

  def readFromFileCSV(path: String): DataFrame = spark.read.csv(path)
  def readFromFileCSVWithDelimiter(path: String, delimiter: String): DataFrame = spark.read.option("delimiter", delimiter).csv(path)
  def readFromFileJSON(path: String): DataFrame = spark.read.json(path)

  def writeToFile(df: DataFrame, path: String): Unit = df.write.format("csv").mode("overwrite").save(path)
}
