  import scala.math._
  object Entropy extends App{
    type D = Double ; type I = Integer; type B = Boolean ; type S = String
    // note: its ok to embed a function in a function if they are coupled as these are
    // I allow integer inputs as it's more convenient, I must cvt to double however.,,.
    //nrTotal is number of training sets considered segmented on some feature,
    /** nrTotal is total training set,of which nrPlus are '+' and 1-nrPlus are '-' */
    def entropy(nrTotal: I, nrPlus: I ):D = {
      def ln2(x: D):D = log10(x)/log10(2)
      val p = nrPlus / nrTotal.toDouble //cvt to double
      //check if p or q is zero, if so, then entropy = 1, **also, print to at least 3 places
      if ( p * (1-p) > 0.0) -1 * (p * ln2(p ) + (1-p) * ln2(1-p) )
      else 0.0
    }//end entropy()
    val test = entropy(30, 29)
    println(s" test $test")
    val entropyStickFigures = entropy(12, 7)
    // using 'C' type formatting, s = String f == float d= integer
    println(f" parent entropyStickFigures, 7 Y, 5 N $entropyStickFigures%1.2f ")
    println(s"""If you segment on body type, then you have two sets, rectangles and ellipsoids
               |In the rectangle set there are 5 y and 1 N -- in the ellipsoidal set 2 y , 4 n
               |So, calc entropy of each and average according to their weights
               |rectangle weight = 6/12 and ellipsoidal weight = 6/12
               |""".stripMargin)
    val eRectangle = entropy(6,5)
    val eEllipsoids = entropy(6,2)
    val avgBodyTypeEntropy = 6/12.0 * eRectangle + 6/12.0 * eEllipsoids
    println(f" avg body type entropy $avgBodyTypeEntropy%1.2f ")
    println(s"""
               |The main purpose of body type segmentation is to see if it further groups features ??
               |The measure of this is called Information Gain (IG and is the difference of the parent entropy and
               |the segmented entropy i.e parent entropy - child entropy
               |""".stripMargin )
    println(f" parent entropy - body type entropy = ${entropyStickFigures -avgBodyTypeEntropy}%2.2f ")
    println(s"Question --- Interpret what this IG means?, what if you segmented on head shape?")
    println(s"""Now suppose there is a particular feature with three subdivisions, such as the residence feature of
Figure 3-5 pg 55 of DS txt : The feature is 'residence status'. Lets see what the information gain is by
segmenting on this feature and its three subdivisions
residence= Rent, residence = Own, residence = Other. Suppose initial population is 30 (16-,14+)
Own = (7-, 1+) ; Rent = (4-, 6+) ; Other (5-, 7+)
Where I interpret '+' as someone who will churn(jump ship) and - as someone who won't..
""".stripMargin)
    val parentEntropy = entropy(30,14)
    println(f"Parent residence entropy $parentEntropy%1.4f ")
    //Fill out the values below --- the special symbol ??? allows a 'place holder option
    val ownEntropy = entropy(8, 1)
    val rentEntropy = entropy(10, 6)
    val otherEntropy = entropy(12, 7)
    val avgResidenceEntropy = 8/30.0 * ownEntropy + 10/30.0 * rentEntropy + 12/30.0 * otherEntropy
    val informationGain = parentEntropy - avgResidenceEntropy
    println(s"Entropy of Own : $ownEntropy")
    println(s"Entropy of Rented : $rentEntropy")
    println(s"Entropy of Other : $otherEntropy")
    println(s"Average Entropy : $avgResidenceEntropy")
    println(s"Information Gain: $informationGain")
  }// end object/**Entropy calculations relevant to Ch 3 DataScience for Business\


