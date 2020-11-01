import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.regression.LinearRegression
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.param.Params
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

case class Person(Customer : String, Age : Int, Income : Double, No_of_cards : Int, Response : Boolean)

object Similarity extends App{
  Logger.getLogger("org").setLevel(Level.OFF)
  type I = Int ; type S = String; type D = Double; type B = Boolean
  val spark = SparkSession.builder().appName("NotRetailData")
    .master("local[*]").getOrCreate()
  spark.conf.set("spark.sql.shuffle.partitions", "5")
  import spark.implicits._
  println(s" Spark version: ${spark.version}")

  val file = "/Users/arvin/Desktop/Assignment11Data.csv"
  val df  = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(file).toDF()

  df.printSchema()
  df.show(5, false)


}
