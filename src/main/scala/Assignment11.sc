import math._

def distance(xs: Array[Double], ys: Array[Double]) = {
  sqrt((xs zip ys).map { case (x,y) => pow(y - x, 2) }.sum)
}


def findKNearestClasses(testPoints: Array[Array[Double]], trainPoints: Array[Array[Double]], k: Int): Array[Int] = {
  testPoints.map { testInstance =>
    val distances =
      trainPoints.zipWithIndex.map { case (trainInstance, c) =>
        c -> distance(testInstance, trainInstance)
      }
    val classes = distances.sortBy(_._2).take(k).map(_._1)
    val classCounts = classes.groupBy(identity).mapValues(_.size)
    classCounts.maxBy(_._2)._1
  }
}
val testInstances = Array(Array(37.0, 50.0, 2.0))
val trainPoints = Array(Array(35.0, 35.0, 3.0), Array(22.0, 50.0, 2.0), Array(64.0, 200.0, 1.0), Array(59.0, 170.0, 1.0), Array(25.0, 40.0, 4.0))


findKNearestClasses(testInstances, trainPoints, k = 3)
// Array(2, 1)
