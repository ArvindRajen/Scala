/**k-Nearest Neighbor similarity calculation ( the simple version )

Based on the DS Fawcett text Ch 6 Table 6.1, where a target person's
label ( 1/0) is estimated by "K" nearest neighbors.

 I have set up a case class with the features ( age, income, Nrcards, label
 plus three extra components, distance, reciprocalDistanceSquared, contribution
 these will be filled out sequentially. I illustrate the  Functional programming mantra
 of not mutating variables, but  create a new person list at each stage. **Check this out ***

 These operations below match what out text does in order to determine what contribution
 each person makes to determining the estimated label for a target person.

 */
package apps
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SQLImplicits, SparkSession, types}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types._

import scala.math._

object Similarity extends App {
  Logger.getLogger("org").setLevel(Level.OFF)
  val spark = SparkSession.builder .master("local[*]").appName("IFT598Similarity") .getOrCreate()
  import spark.implicits._

  //  ****** utility functions that could be created in a separate object,  and then imported in
  def pairDistances ( p : P,  q : P  ):Double = {
    val  (a1, i1, c1)  = (p.age, p.income, p.cards)
    val (a2, i2, c2)  = (q.age, q.income,  q.cards)
    sqrt(pow((a1 - a2), 2) + pow((i1 - i2) ,2) + pow(( c1 - c2), 2) )
  }

  // Cosine Similarity needs both dot product and Norm of the points.
  def cosineSimilarity(p: P, q : P) : Double = {
    val  (a1, i1, c1)  = (p.age, p.income, p.cards)
    val (a2, i2, c2)  = (q.age, q.income,  q.cards)
    val dot = a1*a2 + i1*i2 + c1*c2
    val normP = sqrt(pow(a1,2) + pow(i1,2) + pow(c1,2))
    val normQ = sqrt(pow(a2,2) + pow(i2,2) + pow(c2,2))
    1 - (dot/(normP*normQ))
  }

  // Now, replace the default 0 distance from the target, with the calculated distance.
  def  AddDistances( target: P, persons: List[P]) :List[P] =
    persons. map{ p => { val d =pairDistances( target, p)
      p.copy( distance = d ) } }

  // now given a new person list that has distances filled out, calc and add in the recip dist sqrd
  def AddReciprocalDistances( personsWithDistance : List[P] ) : List[P] = {
    personsWithDistance.map{ p => {
      val d = p.distance
      val recipDist2 =  if (d != 0.0)  1/( d * d) else 0.0
      p.copy( recipDistanceSquared = recipDist2 )
    } }  }

  def SumReciprocalDistances( personsWithReciprocalDistances : List[ P ]) =
    personsWithReciprocalDistances.map{ _.recipDistanceSquared}.sum

  def AddContributions( personsWithReciprocals : List[P], sumRecips : Double ) : List[P] =
    personsWithReciprocals.map{ p =>
      p.copy(contribution = p.recipDistanceSquared/sumRecips)
    }

  // Now, replace the default 0 distance from the target, with the calculated distance.
  def  AddCosines( target: P, persons: List[P]) :List[P] =
    persons. map{ p => { val c =cosineSimilarity( target, p)
      p.copy( cosine = c ) } }

  // ****************  END Utility functions ********************
  val fn = "/Users/arvin/Desktop/similarityDSCh6.csv"
  //> fn  : String = c:/aaprograms/datasets/similarityDSCh6.csv
  val mySchema = StructType( Array(StructField("name", StringType, false),
    StructField("age", DoubleType, false),
    StructField("income", DoubleType, false),
    StructField("cards", DoubleType, false),
    StructField("label", DoubleType, false),
    StructField("distance",DoubleType, false),
    StructField("recipDistanceSquared",DoubleType, false),
    StructField("contribution",DoubleType, false),
    StructField("cosine",DoubleType, false)
  ))
  val rawData = spark.read.format("csv").option("header", "false").schema(mySchema).load(fn)
  rawData.show()
  val ds = rawData.as[P]
  // now I can treat these entries as full featured Java/Scala objects with all Scala code available
  val persons = ds.collect().toList

  val targetDavid = persons(0)
  val personsWithDistances = AddDistances(targetDavid, persons)
  print("\n--------------------------------------------------------")

  personsWithDistances. sortBy( _ .distance)
    .foreach{  p =>  println(f"\n${p.name}%10s | ${p.age}%3.2f | ${p.income}%3.2f | ${p.distance}%4.3f")
    }
  // use the other utility functions ( or your own) to calculate
  // a. the reciprocal squared distances , then the total of these   reciprocal  squared distances, and then the contribution of each
  //. sort and print your result in a
  val personsWithRecip =AddReciprocalDistances( personsWithDistances)
  val sumReciprocals = SumReciprocalDistances( personsWithRecip)
  val personsWithContributions =AddContributions( personsWithRecip, sumReciprocals : Double )
  val personsWithCosinesAdded = AddCosines(targetDavid, personsWithContributions)

  print("\n--------------------------------------------------------")
  val personsWithCosinesSorted =personsWithCosinesAdded. sortBy( p => (p .name, p.cosine ))
    .foreach{  p =>  println(f"\n${p.name}%10s | Distance ${p.distance}%3.2f | Contribution ${p.contribution}%5.2f | Cosine Distance ${p.cosine}%5.3f | Label/Response ${p.label}%2.1f") }

  //now calc the contributions to the yes's and the contributions to the no's
  val contributionsToYes = (personsWithContributions.filter( p => p.label == 1)).map{ p => p.contribution} . sum
  val contributionsToNo = (personsWithContributions.filter( p => p.label == 0)).map{ p => p.contribution} . sum
  print("\n--------------------------------------------------------")
  print(s"\nContribution to Yes For David : $contributionsToYes")
  print(s"\nContribution to No For David : $contributionsToNo")
  if (contributionsToNo > contributionsToYes) print("\nDavid Will Say NO") else print("\nDavid Will Say YES")

}//end similarity
case class P( name: String, age: Double, income : Double, cards: Double,
              label : Double, distance : Double,
              recipDistanceSquared : Double , contribution : Double, cosine : Double)