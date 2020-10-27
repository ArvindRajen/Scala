type I = Integer ; type D = Double; type V = Vector[D]

case class P(x: D, y : D, z: D,density : D, category : I)

val p1 = P(1.0, 3.0, 1.0, 0.1, 1)
val p2 = P(2.0, 4.0, 2.0, 0.1, 1)
val p3 = P(2.0, 2.0, 2.0, 0.2, 1)
val p4 = P(1.0, 3.0, 4.0, 0.2, 1)
val p5 = P(1.0, 3.0, 1.0, 0.2, 1)
val p6 = P(2.0, 1.0, 1.0, 0.15, 1)
val p7 = P(2.0, 3.0, 3.0, 0.25, 0)
val p8 = P(6.0, 2.0, 3.0, 0.4, 0)
val p9 = P(5.0, 3.0, 3.0, 0.3, 0)
val p10 = P(6.0, 4.0, 4.0, 0.45, 0)

val points = Vector(p1,p2,p3,p4,p5,p6,p7,p8,p9,p10)
val OnePoints = points.filter( _.category == 1)
val ZeroPoints = points.filter(_.category == 0 )

val densityMeasure = points.filter(_.density > 2)
// result
def centroid( pts: Vector[P] ) : (D,D,D,D) = {
  val (x,y,z,density)= pts.foldLeft(0.0,0.0,0.0,0.0)(( accum , p) =>
    (accum._1 + p.x , accum._2 + p.y, accum._3 + p.z, accum._4 + p.density))
  val len = pts.length
  (x/len,y/len,z/len,density/len)
}

val tupleofTotal = centroid(points)
// here are centroid coords
val tupleof1 = centroid(OnePoints)
val tupleof0 = centroid(ZeroPoints)

// extract the points from tuples

val (x1,y1,z1,avgDensity1) = centroid(OnePoints)
val (x0,y0,z0,avgDensity0) = centroid(ZeroPoints)

//mid point of line joining centroids
val (xm, ym, zm) = ((x1 + x0)/2.0,(y1+y0)/2.0, (z1+z0)/2.0)

