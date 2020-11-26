import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.{Dataset, DataFrame,Row}
import org.apache.spark.sql.functions._
import org.apache.spark._
object StreamingNetCat {
  def main(args: Array[String]):Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    println("In TestMe 2019-22 rr")
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("StreamNetCat")
      .getOrCreate()

    import spark.implicits._
    println(s" ${spark.version} ")

    // Create DataFrame representing the stream of input lines from the  connection to localhost:9999
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()
    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }//end main()
}//end object StreamingNetCat
