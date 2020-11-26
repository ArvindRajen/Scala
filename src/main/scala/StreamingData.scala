package apps

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.ml.linalg.{ Vectors, Vector}
import org.apache.spark.sql.functions.{window, column, desc, col}

object StreamingData extends App {

  Logger.getLogger("org").setLevel(Level.OFF)
  type I = Int ; type S = String; type D = Double; type B = Boolean
  val spark = SparkSession.builder().appName("NotRetailData")
    .master("local[*]").getOrCreate()
  spark.conf.set("spark.sql.shuffle.partitions", "5")
  import spark.implicits._
  println(s" Spark version: ${spark.version}")

  // Using Read
  val staticDataFrame = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/Users/arvin/Desktop/by-day/*.csv")
  staticDataFrame.createOrReplaceTempView("retail_data")
  val staticSchema = staticDataFrame.schema
  print(s"\n Schema of the Incoming Data : $staticSchema")
  print("\n The DataFrame Version of the 305 Files")

  staticDataFrame.show(5, true)

  print("\n The query of grouping the spending of a customer on CustomerID ")
  staticDataFrame
    .selectExpr(
      "CustomerId",
      "(UnitPrice * Quantity) as total_cost",
      "InvoiceDate")
    .groupBy(
      col("CustomerId"), window(col("InvoiceDate"), "1 day"))
    .sum("total_cost")
    .show(5)

  print("\n Now We Do the Exact Same thing as Above; the difference being that we do it with streaming data ")

  //Now Doing the same for Streaming Data
  val streamingDataFrame = spark.readStream
    .schema(staticSchema)
    .option("maxFilesPerTrigger", 1)
    .format("csv")
    .option("header", "true")
    .load("/Users/arvin/Desktop/by-day/*.csv")

  // in Scala
  val purchaseByCustomerPerHour = streamingDataFrame
    .selectExpr(
      "CustomerId",
      "(UnitPrice * Quantity) as total_cost",
      "InvoiceDate")
    .groupBy(
      $"CustomerId", window($"InvoiceDate", "1 day"))
    .sum("total_cost")

  // in Scala
  purchaseByCustomerPerHour.writeStream
    .format("memory") // memory = store in-memory table
    .queryName("customer_purchases") // the name of the in-memory table
    .outputMode("complete") // complete = all the counts should be in the table
    .start()
  Thread.sleep(5000)

  // in Scala
  spark.sql("""
SELECT *
FROM customer_purchases
ORDER BY `sum(total_cost)` DESC
""")
    .show(5)
}
