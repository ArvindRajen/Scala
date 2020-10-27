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
object LineReg extends App{
  Logger.getLogger("org").setLevel(Level.OFF)
  type I = Int ; type S = String; type D = Double; type B = Boolean
  val spark = SparkSession.builder().appName("NotRetailData")
    .master("local[*]").getOrCreate()
  spark.conf.set("spark.sql.shuffle.partitions", "5")
  import spark.implicits._
  println(s" Spark version: ${spark.version}")

  val dummyPath = "/Users/arvin/Desktop/dummyTimesCategories.txt"
  val rawStrings = spark.sparkContext.textFile(dummyPath)
  println(s" rawStrings.first ${rawStrings.first}, a string with 4 tokens" )
  // need all values to be doubles, zero index is label, others are features
  val trainingData1 = rawStrings.map{ line =>
    val Array(region, noOfParcels, truckAge, deliveryTime ) = line.split("""\s+""")
    (noOfParcels.toDouble, deliveryTime.toDouble)
  }//end map
  val df = trainingData1.toDF(colNames = "noOfParcels", "label")
  df.show(5,false)

  val cols = Array("noOfParcels")

  // VectorAssembler to add feature column
  // input columns - cols
  // feature column - feature
  val assembler = new VectorAssembler()
    .setInputCols(cols)
    .setOutputCol("features")

  val labelDf = assembler.transform(df)
  labelDf.printSchema()
  labelDf.show(5,false)

  val seed = 5043
  val Array(trainingData, testData) = labelDf.randomSplit(Array(0.7, 0.3), seed)

  // train Linear regression model with training data set
  val linearRegression = new LinearRegression()
  val linearRegressionModel = linearRegression.fit(trainingData)

//  // run model with test data set to get predictions
//  // this will add new columns rawPrediction, probability and prediction
//  val prediction = linearRegressionModel.transform(testData)
//  prediction.show(10)
//
//  // Print the coefficients and intercept for linear regression
//  println(s"Coefficients: ${linearRegressionModel.coefficients} Intercept: ${linearRegressionModel.intercept}")
//
//  // Summarize the model over the training set and print out some metrics
//  val trainingSummary = linearRegressionModel.summary
//
//  //  trainingSummary.residuals.show()
//  println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
//  println(s"r2: ${trainingSummary.r2}")

}
