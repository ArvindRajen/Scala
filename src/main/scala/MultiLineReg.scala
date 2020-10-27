import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.param.Params
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import scala.math.{pow, sqrt}
object MultiLineReg extends App{
  Logger.getLogger("org").setLevel(Level.OFF)
  type I = Int ; type S = String; type D = Double; type B = Boolean
  val spark = SparkSession.builder().appName("NotRetailData")
    .master("local[*]").getOrCreate()
  spark.conf.set("spark.sql.shuffle.partitions", "5")
  import spark.implicits._
  println(s" Spark version: ${spark.version}")

  val quizCSV = "/Users/arvin/Desktop/QuizPoints.csv"
  val df  = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(quizCSV).toDF()

  df.printSchema()
  df.show(5, false)

  val cols = Array("x1", "x2", "x3", "treetype")

  // VectorAssembler to add feature column
  // input columns - cols
  // feature column - feature
  val assembler = new VectorAssembler()
    .setInputCols(cols)
    .setOutputCol("features")
  val labelDf = assembler.transform(df)
  labelDf.printSchema()
  labelDf.show(5,false)

  // train linear regression model with training data set
  val linearRegressionFull = new LinearRegression()
  val linearRegressionModelFull = linearRegressionFull.fit(labelDf)

  val trainingPredictionFull = linearRegressionModelFull.transform(labelDf)
  trainingPredictionFull.show(10)

  val residuals = trainingPredictionFull.withColumn("Residuals", (trainingPredictionFull("label") -
    trainingPredictionFull("prediction")))
  residuals.show((10))

  //   Print the coefficients and intercept for linear regression
    println(s"Coefficients: ${linearRegressionModelFull.coefficients} Intercept: ${linearRegressionModelFull.intercept}")

  //   Summarize the model over the training set and print out some metrics
  val trainingSummaryFull = linearRegressionModelFull.summary

  //  trainingSummary.residuals.show()
  println(s"RMSE: ${trainingSummaryFull.rootMeanSquaredError}")
  println(s"MSE: ${trainingSummaryFull.meanSquaredError}")
  println(s"r2: ${trainingSummaryFull.r2}")
  println(s"Correlation: ${sqrt(trainingSummaryFull.r2)}")
}