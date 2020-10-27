import scala.math._
import scala.io.Source
type D = Double ; type I = Integer; type B = Boolean ; type S = String

def entropy(nrTotal: I, nrPlus: I ):D = {
    def ln2(x: D):D = log10(x)/log10(2)
    val p = nrPlus / nrTotal.toDouble //cvt to double
    //check if p or q is zero, if so, then entropy = 1, **also, print to at least 3 places
    if ( p * (1-p) > 0.0) -1 * (p * ln2(p ) + (1-p) * ln2(1-p) )
    else 0.0
}//end entropy()

val path = "/Users/arvin/Documents/4th Sem/IFT 598 - Big Data/stickFigureEntropy.csv"
val rawData = Source.fromFile(path).getLines.drop(1).toList

// Taking the 4 columns
val pairs = rawData.map{ line => {
  val Array(head, body, color, label) = line.split(",")
  (head.toString, body.toString, color.toString, label.toString)}}

val headShape = pairs.map{ case(head, body,color,label) => (head,label)}//head pair
val bodyShape = pairs.map{ case(head, body,color,label) => (body,label)}//body pair
val colorBody = pairs.map{ case(head, body,color,label) => (color,label)}//color pair
val finalLabel = pairs.map{ case(head, body,color,label) => label}//label Column

headShape.groupBy(identity).mapValues(_.size)
val headpos = res0.values.mkString(",")

bodyShape.groupBy(identity).mapValues(_.size)
val bodypos = res1.values.mkString(",")

colorBody.groupBy(identity).mapValues(_.size)
val colorpos = res2.values.mkString(",")

finalLabel.groupBy(identity).mapValues(_.size)
val labelpos = res3.values.mkString(",")

val parentEntropy = entropy(12,7)

/** Question 1 */

val squareEntropy = entropy(9,5)
val cicrleEntropy = entropy(3,2)
val headEntropy = 9/12.0*squareEntropy + 3/12.0*cicrleEntropy
val infoGainQ1 = parentEntropy - headEntropy

/** Question 2 */

val rectEntropy = entropy(6,5)
val elliEntropy = entropy(6,2)
val bodyEntropy = 6/12.0*rectEntropy + 6/12.0*elliEntropy
val infoGainQ2 = parentEntropy - bodyEntropy

/** Question 3 */
val whiteEntropy = entropy(10,6)
val greyEntropy = entropy(2,1)
val colorEntropy = 10/12.0*rectEntropy + 2/12.0*elliEntropy
val infoGainQ3 = parentEntropy - colorEntropy

/** Question 4 */
//segmenting on head type from top
val bodySeg = pairs.map{ case(head, body,color,label) => (body,head,label)}
bodySeg.groupBy(identity).mapValues(_.size)

print(s"Information Gain after segmenting on body $infoGainQ2")

val BodyRectHeadSquEntropy = entropy(5,5)
val BodyRectHeadCirEntropy = entropy(1,0)
val BodyElliHeadCirEntropy = entropy(2,2)
val BodyElliHeadSquEntropy = entropy(4,0)

val segEntropy = 5/6.0* BodyRectHeadSquEntropy + 1/6.0*BodyRectHeadCirEntropy +
  2/6.0*BodyElliHeadCirEntropy + 4/6.0*BodyElliHeadSquEntropy

val infGainAfterSeg = bodyEntropy - segEntropy