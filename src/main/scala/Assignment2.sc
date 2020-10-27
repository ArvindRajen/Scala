type S = String
type D = Double
type I = Int
type V = Vector[D]
import scala.io.Source
import scala.math._

def VectorSubtraction(x : V, y: V):V = (x zip y).map{case (x,y) => x-y}
def multiplyByScalar(x : V, y: D): V = x.map{_ * y}
def meanval(x:V) : D = x.sum/x.length
def dotProduct(x : V, y: V) : D = (x zip y).map{case (x,y) => x*y}.sum
def norm(x:V) : D = sqrt(dotProduct(x,x))
def centralize(x :V) : V = x.map{_ - meanval(x)}
def regCoeff(x : V, y: V): D = dotProduct(x,y)/pow(norm(x),2)
def regressionInter(x : V, y: V):D = meanval(y) - regCoeff(x,y)*meanval(x)
def LinearReg(x : V, y: V): Unit = {
  val xc: V = centralize(x)
  val yc: V = centralize(y)
  val intercept = regressionInter(x,y)
  val RegCoeff = regCoeff(xc,yc)
  val SST = dotProduct(yc,yc)
  val error = VectorSubtraction(yc, multiplyByScalar(xc, RegCoeff))
  val SSE = dotProduct(error, error)
  val MSE = SSE/error.length
  val RMSE = sqrt(MSE)
  val predicted = multiplyByScalar(xc, RegCoeff)
  val SSR = dotProduct(predicted, predicted)
  println(s"The regression coefficient is : $RegCoeff")
  println(s"The regression intercept is : $intercept")
  println(s"The SST is : $SST")
  println(s"The SSR is : $SSR")
  println(s"The SSE is : $SSE")
  println(s"The MSE is : $MSE")
  println(s"The RMSE is : $RMSE")
}

val X1 = Vector(1.0,2.0,2.0,1.0,1.0,2.0,2.0,6.0,5.0,6.0)
val X2 = Vector(3.0,4.0,2.0,3.0,3.0,1.0,3.0,2.0,3.0,4.0)
val X3 = Vector(1.0,2.0,2.0,4.0,1.0,1.0,3.0,3.0,3.0,4.0)
val treetype = Vector(1.0,1.0,1.0,1.0,1.0,0.0,0.0,0.0,0.0,0.0)


val Y = Vector(0.1,0.1,0.2,0.2,0.2,0.15,0.25,0.4,0.3,0.45)

val prob1 = LinearReg(X1,Y)
val prob2 = LinearReg(X2,Y)
val prob3 = LinearReg(X3,Y)
val prob4 = LinearReg(treetype,Y)