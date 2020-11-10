package apps

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.ml.linalg.{ Vectors, Vector}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.evaluation.MulticlassMetrics

object Assignment11 extends App{

  Logger.getLogger("org").setLevel(Level.OFF)
  type I = Int ; type S = String; type D = Double; type B = Boolean
  val spark = SparkSession.builder().appName("NotRetailData")
    .master("local[*]").getOrCreate()
  spark.conf.set("spark.sql.shuffle.partitions", "5")
  import spark.implicits._
  println(s" Spark version: ${spark.version}")

  val hosmerCSV = "/Users/arvin/Desktop/HosmerCHD-1.csv"
  val df  = spark.read.format("csv")
    .option("header", "false")
    .option("inferSchema", "true")
    .load(hosmerCSV).toDF("Group", "Age", "label")

  df.printSchema()
  df.show(10, false)

  val cols = Array("Age")

  // VectorAssembler to add feature column
  // input columns - cols
  // feature column - feature
  val assembler = new VectorAssembler()
    .setInputCols(cols)
    .setOutputCol("features")
  val labelDf = assembler.transform(df)
  labelDf.printSchema()
  labelDf.show(10,false)

  // train logistic regression model with training data set
  val logisticRegression = new LogisticRegression()
  val logisticRegressionModel = logisticRegression.fit(labelDf)

  // Print the coefficients and intercept for linear regression
  println(s"Coefficients: ${logisticRegressionModel.coefficients} " +
    s"\n Intercept: ${logisticRegressionModel.intercept}")


}//end Assignment11

