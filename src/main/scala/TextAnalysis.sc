import scala.math._
type I = Int; type D = Double; type S= String
type V = Vector[D]   // see tfidf vector for this usage

//  **************************************
def dot(u:V,  w:V):D =
  (u zip w).map{case(s,t) => s * t  }.sum
def norm(v: V):D = sqrt(dot(v,v))
def cos(v:V, w:V):D = dot(v,w)/(norm(v)* norm(w))
def angle(v:V, w:V):D = acos( cos(v,w)) * 180.0/ Pi
//   ****************************************
val corpusVectorLength = 24
case class TestDoc ( text: S,
                     nrTokens:I=0,
                     tf    : Map[I,I]        = Map(),
                     stdTF : Map[Int,Double] = Map(),
                     nrDocs   : Map[I,I]     =Map(),
                     NCorpus : I = 0,
                     idf   : Map[I, D]       =Map(),
                     tfidf : Map[I,D]        =Map()
                   )//end TestDoc

def TFfn(text: S ):TestDoc ={
  val tokenArray = text.trim.split("\\s+")
  val nrTokens = tokenArray.length
  val hashes = tokenArray.map{e => abs(e.hashCode())%corpusVectorLength}
  //tf is now a Map(index, frequency) not just a number
  val tf= hashes.groupBy(identity).map{case (k,v) => (k, v.length)}
  // now insert that tf 'Map' into a new document as below
  TestDoc(text ,nrTokens, tf)
}//end TFfn

def STDTFfn( doc: TestDoc):TestDoc = {
  val stdTF = doc.tf.map{ case (index,value) =>
    (index, value/doc.nrTokens.toDouble)}
  doc.copy(stdTF =  stdTF )
}// end STDTFfn

def NRDocsfn( doc: TestDoc,STDTFDocList:List[TestDoc] ):TestDoc= {
  val nrMaps = for {
    (k, v) <- doc.stdTF
    m <- STDTFDocList
    if m.stdTF.contains(k)
  } yield Map(k -> 1)
  val NCorpus = STDTFDocList.size
  val a = nrMaps.flatten
  val nrDocs = a.groupBy(_._1).mapValues(_.map(_._2).size)
  doc.copy(nrDocs = nrDocs, NCorpus = NCorpus)
}//end NRDocsfn

def IDFfn(doc :TestDoc):TestDoc = {
  val idfmaps = for{  (k,v) <- doc.nrDocs
    // v is nr of docs this k hash appears in
                      } yield (k , log10(doc.NCorpus/v.toDouble))
  doc.copy(idf = idfmaps)
}

def TFIDF( doc: TestDoc):TestDoc = {
  //v is already the idf= log10(N/nrDocs)
  // while std is the value of the standardized tf
  val tfidfProduct = for  { (k,v) <- doc.idf
                            std = doc.stdTF(k)
                            } yield (k , v * std)
  doc.copy(tfidf = tfidfProduct)
}

def DocsCosine ( docA:TestDoc, docB:TestDoc):D = {
  val pairsProds = for {(k, v) <- docA.tfidf
                        if docB.tfidf.contains(k)
                        } yield (v , docB.tfidf(k))
  val (x,y )= pairsProds.toVector.unzip
  if(x.isEmpty)0.0
  else {
    cos(x,y)
  }
}

//   ____________Manual calculations _________________

val text0 = "now is the only time there now is"
val text1 = "time runs slower with faster runs"
val text2 = "a clock runs slower and slower"

val corpusTextList = List(text0, text1, text2)
val d0 = STDTFfn(TFfn(text0))
val d1 = STDTFfn(TFfn(text1))
val d2 = STDTFfn(TFfn(text2))

val STDTFDocList = List(d0,d1,d2)

val d0nr = NRDocsfn(d0, STDTFDocList)
val d1nr = NRDocsfn(d1, STDTFDocList)
val d2nr = NRDocsfn(d2 ,STDTFDocList)

val d0idf = IDFfn(d0nr)
val d1idf = IDFfn(d1nr)
val d2idf = IDFfn(d2nr)

val tfidfDoc0 = TFIDF(d0idf)
val tfidfDoc1 = TFIDF(d1idf)
val tfidfDoc2 = TFIDF(d2idf)

val cos01= DocsCosine(tfidfDoc0, tfidfDoc1)
val cos02= DocsCosine(tfidfDoc0, tfidfDoc2)
val cos12= DocsCosine(tfidfDoc1, tfidfDoc2)


