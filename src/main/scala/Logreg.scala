import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.param.Params
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import plotly._, element._, layout._, Plotly._

object Logreg extends App{
  Logger.getLogger("org").setLevel(Level.OFF)
  type I = Int ; type S = String; type D = Double; type B = Boolean
  val spark = SparkSession.builder().appName("RetailData")
    .master("local[*]").getOrCreate()
  spark.conf.set("spark.sql.shuffle.partitions", "5")
  import spark.implicits._
  println(s" Spark version: ${spark.version}")

  val hosmerPath = "/Users/arvin/Desktop/Hosmer.txt"
  val rawStrings = spark.sparkContext.textFile(hosmerPath)
  println(s" rawStrings.first ${rawStrings.first}, a string with 3 tokens" )
  // need all values to be doubles, zero index is label, others are features
  val trainingData1 = rawStrings.map{ line =>
    val Array(groupNo,age,label) = line.split("""\s+""")
    (groupNo.toDouble, age.toDouble, label.toDouble)
  }//end map
  val df = trainingData1.toDF()
  df.show(5,false)

  val cols = Array("_1", "_2")

  // VectorAssembler to add feature column
  // input columns - cols
  // feature column - features
  val assembler = new VectorAssembler()
    .setInputCols(cols)
    .setOutputCol("features")
  val featureDf = assembler.transform(df)
  featureDf.printSchema()

  featureDf.show(5,false)

  val indexer = new StringIndexer()
    .setInputCol("_3")
    .setOutputCol("label")
  val labelDf = indexer.fit(featureDf).transform(featureDf)
  labelDf.printSchema()
  labelDf.show(5,false)

  val seed = 5043
  val Array(trainingData, testData) = labelDf.randomSplit(Array(0.7, 0.3), seed)

  // train logistic regression model with training data set
  val logisticRegression = new LogisticRegression()
  println("LogisticRegression parameters:\n" + logisticRegression.explainParams() + "\n")

  val logisticRegressionModel = logisticRegression.fit(trainingData)

  // run model with test data set to get predictions
  // this will add new columns rawPrediction, probability and prediction
  val prediction = logisticRegressionModel.transform(testData)
  prediction.show(10)

  // evaluate model with area under ROC
  val evaluator = new BinaryClassificationEvaluator()
    .setLabelCol("label")
    .setRawPredictionCol("prediction")
    .setMetricName("areaUnderROC")

  // measure the accuracy
  val accuracy = evaluator.evaluate(prediction)
  println(accuracy)

  // Print the coefficients and intercept for linear regression
  println(s"Coefficients: ${logisticRegressionModel.coefficients} Intercept: ${logisticRegressionModel.intercept}")


}

