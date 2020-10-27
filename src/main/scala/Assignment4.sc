import scala.io.Source
import scala.math._
import Entropy._

val path = "/Users/arvin/Documents/4th Sem/IFT 598 - Big Data/dummyTimesCategories.txt"
val rawData = Source.fromFile(path).getLines().toList
//Sorting by name
val sortedData = rawData.sorted
println("The empty list is: " + sortedData)

// I only want the age and income
val pairs = rawData.map{ line => {
val Array(s, x, y, v) = line.split(",")
(x.toDouble, y.toDouble)}}

// I want to sort my tuples on their first component ' X ' values
println(s"Initial separateTuples $pairs")
val sortedTuples = pairs.sortBy(_._1)
println(s"sortedTuples $sortedTuples ")
val rawXs = pairs.map { case(x,y) => x}//Age Column
val rawYs = pairs.map { case(x,y) => y}//Income Column

//Vector Conversion to run in our previous code.
val X = rawXs.toVector
val Y = rawYs.toVector

type S = String
type D = Double
type I = Int
type V = Vector[D]

def VectorSubtraction(x : V, y: V):V = (x zip y).map{case (x,y) => x-y}
def multiplyByScalar(x : V, y: D): V = x.map{_ * y}
def meanval(x:V) : D = x.sum/x.length
def dotProduct(x : V, y: V) : D = (x zip y).map{case (x,y) => x*y}.sum
def norm(x:V) : D = sqrt(dotProduct(x,x))
def centralize(x :V) : V = x.map{_ - meanval(x)}
def regCoeff(x : V, y: V): D = dotProduct(x,y)/dotProduct(x,x)
def corrCoeff (x : V, y: V):D = dotProduct(x,y)/(norm(x)*norm(y))
def angle (x : V, y: V):D =  acos(corrCoeff(x,y))*180/math.Pi

def LinearReg(x : V, y: V): Unit = {
  val xc: V = centralize(x)
  val yc: V = centralize(y)
  val RegCoeff = regCoeff(xc,yc)
  val intercept = meanval(y) - RegCoeff*meanval(x)
  val SST = dotProduct(yc,yc)
  val error = VectorSubtraction(yc, multiplyByScalar(xc, RegCoeff))
  val SSE = dotProduct(error, error)
  val MSE = SSE/error.length
  val RMSE = sqrt(MSE)
  val predicted = multiplyByScalar(xc, RegCoeff)
  val SSR = dotProduct(predicted, predicted)
  val CorrCoeff = corrCoeff(xc,yc)
  val theta = angle(xc,yc)
  val r2 = pow(CorrCoeff,2)

  println(s"The regression coefficient is : $RegCoeff")
  println(s"The regression intercept is : $intercept")
  println(s"The SST is : $SST")
  println(s"The SSR is : $SSR")
  println(s"The SSE is : $SSE")
  println(s"The MSE is : $MSE")
  println(s"The RMSE is : $RMSE")
  println(s"The Correlation Coefficient is : $CorrCoeff")
  println(s"The Angle between the centered vectors is : $theta")
  println(s"The R2 of the model is : $r2")

}

val prob1 = LinearReg(X,Y)


