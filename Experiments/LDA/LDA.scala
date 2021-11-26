package LDA

import edu.stanford.nlp.process.Morphology
import edu.stanford.nlp.simple.Document
import kmeans.RunKMeans.allConfigurationsSetKmeans
import org.apache.log4j.{Level, Logger}

import scala.collection.JavaConversions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.clustering.{DistributedLDAModel, EMLDAOptimizer, LDA, OnlineLDAOptimizer}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

import java.io.{BufferedWriter, File, FileWriter}
import scala.collection.mutable.HashMap

case class Params(
                   input: String = "",
                   k: Int = 5,
                   maxIterations: Int = 10,
                   docConcentration: Double = -1,
                   topicConcentration: Double = -1,
                   vocabSize: Int = 2900000,
                   stopwordFile: String = "/home/velisarios/stopWords.txt",
                   algorithm: String = "em",
                   checkpointDir: Option[String] = None,
                   checkpointInterval: Int = 10)

class LDAExample(sc: SparkContext, spark: SparkSession) {

  def run(params: Params): Unit = {

    Logger.getRootLogger.setLevel(Level.WARN)

    // Load documents, and prepare them for LDA.
    val preprocessStart = System.nanoTime()
    val (corpus, vocabArray, actualNumTokens) =
      preprocess(sc, params.input, params.vocabSize, params.stopwordFile)
    val actualCorpusSize = corpus.count()
    val actualVocabSize = vocabArray.length
    val preprocessElapsed = (System.nanoTime() - preprocessStart) / 1e9
    corpus.cache()


    // Run LDA.
    val lda = new LDA()

    val optimizer = params.algorithm.toLowerCase match {
      case "em" => new EMLDAOptimizer
      // add (1.0 / actualCorpusSize) to MiniBatchFraction be more robust on tiny datasets.
      case "online" => new OnlineLDAOptimizer().setMiniBatchFraction(0.05 + 1.0 / actualCorpusSize)
      case _ => throw new IllegalArgumentException(
        s"Only em, online are supported but got ${params.algorithm}.")
    }

    lda.setOptimizer(optimizer)
      .setK(params.k)
      .setMaxIterations(params.maxIterations)
      .setDocConcentration(params.docConcentration)
      .setTopicConcentration(params.topicConcentration)
      .setCheckpointInterval(params.checkpointInterval)
    if (params.checkpointDir.nonEmpty) {
      sc.setCheckpointDir(params.checkpointDir.get)
    }
    val startTime = System.nanoTime()
    val ldaModel = lda.run(corpus)
    val elapsed = (System.nanoTime() - startTime) / 1e9



    if (ldaModel.isInstanceOf[DistributedLDAModel]) {
      val distLDAModel = ldaModel.asInstanceOf[DistributedLDAModel]
      val avgLogLikelihood = distLDAModel.logLikelihood / actualCorpusSize.toDouble

    }



    sc.stop()
  }

  import org.apache.spark.sql.functions._

  /**
   * Load documents, tokenize them, create vocabulary, and prepare documents as term count vectors.
   *
   * @return (corpus, vocabulary as array, total token count in corpus)
   */
  def preprocess(
                  sc: SparkContext,
                  paths: String,
                  vocabSize: Int,
                  stopwordFile: String): (RDD[(Long, Vector)], Array[String], Long) = {

    import spark.implicits._
    //Reading the Whole Text Files
    val initialrdd = spark.sparkContext.wholeTextFiles(paths).map(_._2)
    initialrdd.cache()
    val rdd = initialrdd.mapPartitions { partition =>
      val morphology = new Morphology()
      partition.map { value =>
        LDAHelper.getLemmaText(value, morphology)
      }
    }.map(LDAHelper.filterSpecialCharacters)
    rdd.cache()
    initialrdd.unpersist()
    val df = rdd.toDF("docs")
    val customizedStopWords: Array[String] = if (stopwordFile.isEmpty) {
      Array.empty[String]
    } else {
      val stopWordText = sc.textFile(stopwordFile).collect()
      stopWordText.flatMap(_.stripMargin.split(","))
    }
    //Tokenizing using the RegexTokenizer
    val tokenizer = new RegexTokenizer().setInputCol("docs").setOutputCol("rawTokens")

    //Removing the Stop-words using the Stop Words remover
    val stopWordsRemover = new StopWordsRemover().setInputCol("rawTokens").setOutputCol("tokens")
    stopWordsRemover.setStopWords(stopWordsRemover.getStopWords ++ customizedStopWords)

    //Converting the Tokens into the CountVector
    val countVectorizer = new CountVectorizer().setVocabSize(vocabSize).setInputCol("tokens").setOutputCol("features")

    //Setting up the pipeline
    val pipeline = new Pipeline().setStages(Array(tokenizer, stopWordsRemover, countVectorizer))

    val model = pipeline.fit(df)
    val documents = model.transform(df).select("features").rdd.map {
      case Row(features: MLVector) => Vectors.fromML(features)
    }.zipWithIndex().map(_.swap)

    (documents,
      model.stages(2).asInstanceOf[CountVectorizerModel].vocabulary, // vocabulary
      documents.map(_._2.numActives).sum().toLong) // total token count
  }
}

object LDA extends App {
  val configuration1: HashMap[String, String] = HashMap(("spark.serializer", "org.apache.spark.serializer.KryoSerializer"))

  val configuration2: HashMap[String, String] = HashMap(("spark.memory.storageFraction", "0.3"))

  val configuration3: HashMap[String, String] = HashMap(("spark.memory.storageFraction", "0.1"))
  val configuration4: HashMap[String, String] = HashMap(("spark.memory.storageFraction", "0.7"))
  val configuration5: HashMap[String, String] = HashMap(("spark.reducer.maxSizeInFligh", "72m"))
  val configuration6: HashMap[String, String] = HashMap(("spark.reducer.maxSizeInFligh", "24m"))
  val configuration7: HashMap[String, String] = HashMap(("spark.shuffle.file.buffer", "48k"))
  val configuration8: HashMap[String, String] = HashMap(("spark.shuffle.file.buffer", "16k"))
  val configuration9: HashMap[String, String] = HashMap(("spark.shuffle.compress", "false"))
  val configuration10: HashMap[String, String] = HashMap(("spark.io.compression.codec", "lzf"))
  val configuration11: HashMap[String, String] = HashMap(("spark.rdd.compress", "true"))
  val configuration12: HashMap[String, String] = HashMap(("spark.shuffle.io.preferDirectBufs", "false"))
  val configuration13: HashMap[String, String] = HashMap(("spark.shuffle.spill.compress", "false"))

  val configurationsSet1_2 = HashMap[String, String](("spark.serializer", "org.apache.spark.serializer.KryoSerializer"), ("spark.memory.storageFraction", "0.3"))

  val configurationsSet1_3 = HashMap[String, String](("spark.serializer", "org.apache.spark.serializer.KryoSerializer"), ("spark.memory.storageFraction", "0.1"))

  val configurationsSet1_4 = HashMap[String, String](("spark.serializer", "org.apache.spark.serializer.KryoSerializer"), ("spark.memory.storageFraction", "0.7"))

  val configurationsSet1_5 = HashMap[String, String](("spark.serializer", "org.apache.spark.serializer.KryoSerializer"), ("spark.reducer.maxSizeInFlight", "72m"))

  val configurationsSet1_6 = HashMap[String, String](("spark.serializer", "org.apache.spark.serializer.KryoSerializer"), ("spark.reducer.maxSizeInFlight", "24m"))

  val configurationsSet1_7 = HashMap[String, String](("spark.serializer", "org.apache.spark.serializer.KryoSerializer"), ("spark.shuffle.file.buffer", "48k"))

  val configurationsSet1_8 = HashMap[String, String](("spark.serializer", "org.apache.spark.serializer.KryoSerializer"), ("spark.shuffle.file.buffer", "16k"))

  val configurationsSet1_9 = HashMap[String, String](("spark.serializer", "org.apache.spark.serializer.KryoSerializer"), ("spark.shuffle.compress", "false"))

  val configurationsSet1_10 = HashMap[String, String](("spark.serializer", "org.apache.spark.serializer.KryoSerializer"), ("spark.io.compression.codec", "lzf"))

  val configurationsSet1_11 = HashMap[String, String](("spark.serializer", "org.apache.spark.serializer.KryoSerializer"), ("spark.rdd.compress", "true"))

  val configurationsSet1_12 = HashMap[String, String](("spark.serializer", "org.apache.spark.serializer.KryoSerializer"), ("spark.shuffle.io.preferDirectBufs", "false"))

  val configurationsSet1_13 = HashMap[String, String](("spark.serializer", "org.apache.spark.serializer.KryoSerializer"), ("spark.shuffle.spill.compress", "false"))


  //"spark.memory.storageFraction", "0.3"), ("spark.reducer.maxSizeInFlight", "72m"
  val configurationsSet2_5 = HashMap[String, String](("spark.memory.storageFraction", "0.3"), ("spark.reducer.maxSizeInFlight", "72m"))

  //  "spark.memory.storageFraction", "0.3"), ("spark.reducer.maxSizeInFlight", 24m
  val configurationsSet2_6 = HashMap[String, String](("spark.memory.storageFraction", "0.3"), ("spark.reducer.maxSizeInFlight", "24m"))

  //"spark.memory.storageFraction", "0.3") with spark.shuffle.file.buffer: 48k
  val configurationsSet2_7 = HashMap[String, String](("spark.memory.storageFraction", "0.3"), ("spark.shuffle.file.buffer", "48k"))

  //"spark.memory.storageFraction", "0.3") with spark.shuffle.file.buffer: 16k
  val configurationsSet2_8 = HashMap[String, String](("spark.memory.storageFraction", "0.3"), ("spark.shuffle.file.buffer", "16k"))

  //"spark.memory.storageFraction", "0.3") with spark.shuffle.compress: false
  val configurationsSet2_9 = HashMap[String, String](("spark.memory.storageFraction", "0.3"), ("spark.shuffle.compress", "false"))

  //"spark.memory.storageFraction", "0.3") with spark.io.compression.codec: lzf
  val configurationsSet2_10 = HashMap[String, String](("spark.memory.storageFraction", "0.3"), ("spark.io.compression.codec", "lzf"))

  //"spark.memory.storageFraction", "0.3") with spark.rdd.compress: true
  val configurationsSet2_11 = HashMap[String, String](("spark.memory.storageFraction", "0.3"), ("spark.rdd.compress", "true"))

  //"spark.memory.storageFraction", "0.3") with spark.shuffle.io.preferDirectBufs: false
  val configurationsSet2_12 = HashMap[String, String](("spark.memory.storageFraction", "0.3"), ("spark.shuffle.io.preferDirectBufs", "false"))

  //"spark.memory.storageFraction", "0.3") with spark.shuffle.spill.compress: false
  val configurationsSet2_13 = HashMap[String, String](("spark.memory.storageFraction", "0.3"), ("spark.shuffle.spill.compress", "false"))

  //====== "spark.memory.storageFraction", "0.1" row ======//

  //"spark.memory.storageFraction", "0.1"), ("spark.reducer.maxSizeInFlight", "72m"
  val configurationsSet3_5 = HashMap[String, String](("spark.memory.storageFraction", "0.1"), ("spark.reducer.maxSizeInFlight", "72m"))

  //  "spark.memory.storageFraction", "0.1"), ("spark.reducer.maxSizeInFlight", 24m
  val configurationsSet3_6 = HashMap[String, String](("spark.memory.storageFraction", "0.1"), ("spark.reducer.maxSizeInFlight", "24m"))

  //"spark.memory.storageFraction", "0.1") with spark.shuffle.file.buffer: 48k
  val configurationsSet3_7 = HashMap[String, String](("spark.memory.storageFraction", "0.1"), ("spark.shuffle.file.buffer", "48k"))

  //"spark.memory.storageFraction", "0.3") with spark.shuffle.file.buffer: 16k
  val configurationsSet3_8 = HashMap[String, String](("spark.memory.storageFraction", "0.1"), ("spark.shuffle.file.buffer", "16k"))

  //"spark.memory.storageFraction", "0.3") with spark.shuffle.compress: false
  val configurationsSet3_9 = HashMap[String, String](("spark.memory.storageFraction", "0.1"), ("spark.shuffle.compress", "false"))

  //"spark.memory.storageFraction", "0.3") with spark.io.compression.codec: lzf
  val configurationsSet3_10 = HashMap[String, String](("spark.memory.storageFraction", "0.1"), ("spark.io.compression.codec", "lzf"))

  //"spark.memory.storageFraction", "0.3") with spark.rdd.compress: true
  val configurationsSet3_11 = HashMap[String, String](("spark.memory.storageFraction", "0.1"), ("spark.rdd.compress", "true"))

  //"spark.memory.storageFraction", "0.3") with spark.shuffle.io.preferDirectBufs: false
  val configurationsSet3_12 = HashMap[String, String](("spark.memory.storageFraction", "0.1"), ("spark.shuffle.io.preferDirectBufs", "false"))

  //"spark.memory.storageFraction", "0.3") with spark.shuffle.spill.compress: false
  val configurationsSet3_13 = HashMap[String, String](("spark.memory.storageFraction", "0.1"), ("spark.shuffle.spill.compress", "false"))



  //====== "spark.memory.storageFraction", "0.7" row ======//


  val configurationsSet4_5 = HashMap[String, String](("spark.memory.storageFraction", "0.7"), ("spark.reducer.maxSizeInFlight", "72m"))


  val configurationsSet4_6 = HashMap[String, String](("spark.memory.storageFraction", "0.7"), ("spark.reducer.maxSizeInFlight", "24m"))


  val configurationsSet4_7 = HashMap[String, String](("spark.memory.storageFraction", "0.7"), ("spark.shuffle.file.buffer", "48k"))


  val configurationsSet4_8 = HashMap[String, String](("spark.memory.storageFraction", "0.7"), ("spark.shuffle.file.buffer", "16k"))


  val configurationsSet4_9 = HashMap[String, String](("spark.memory.storageFraction", "0.7"), ("spark.shuffle.compress", "false"))


  val configurationsSet4_10 = HashMap[String, String](("spark.memory.storageFraction", "0.7"), ("spark.io.compression.codec", "lzf"))


  val configurationsSet4_11 = HashMap[String, String](("spark.memory.storageFraction", "0.7"), ("spark.rdd.compress", "true"))


  val configurationsSet4_12 = HashMap[String, String](("spark.memory.storageFraction", "0.7"), ("spark.shuffle.io.preferDirectBufs", "false"))


  val configurationsSet4_13 = HashMap[String, String](("spark.memory.storageFraction", "0.7"), ("spark.shuffle.spill.compress", "false"))


  //====== spark.reducer.maxSizeInFlight: 72m row ======//











  val configurationsSet5_7 = HashMap[String, String](("spark.reducer.maxSizeInFlight", "72m"), ("spark.shuffle.file.buffer", "48k"))


  val configurationsSet5_8 = HashMap[String, String](("spark.reducer.maxSizeInFlight", "72m"), ("spark.shuffle.file.buffer", "16k"))


  val configurationsSet5_9 = HashMap[String, String](("spark.reducer.maxSizeInFlight", "72m"), ("spark.shuffle.compress", "false"))


  val configurationsSet5_10 = HashMap[String, String](("spark.reducer.maxSizeInFlight", "72m"), ("spark.io.compression.codec", "lzf"))


  val configurationsSet5_11 = HashMap[String, String](("spark.reducer.maxSizeInFlight", "72m"), ("spark.rdd.compress", "true"))


  val configurationsSet5_12 = HashMap[String, String](("spark.reducer.maxSizeInFlight", "72m"), ("spark.shuffle.io.preferDirectBufs", "false"))

  val configurationsSet5_13 = HashMap[String, String](("spark.reducer.maxSizeInFlight", "72m"), ("spark.shuffle.spill.compress", "false"))


  //====== spark.reducer.maxSizeInFlight: 24m row ======//




  val configurationsSet6_7 = HashMap[String, String](("spark.reducer.maxSizeInFlight", "24m"), ("spark.shuffle.file.buffer", "48k"))


  val configurationsSet6_8 = HashMap[String, String](("spark.reducer.maxSizeInFlight", "24m"), ("spark.shuffle.file.buffer", "16k"))


  val configurationsSet6_9 = HashMap[String, String](("spark.reducer.maxSizeInFlight", "24m"), ("spark.shuffle.compress", "false"))


  val configurationsSet6_10 = HashMap[String, String](("spark.reducer.maxSizeInFlight", "24m"), ("spark.io.compression.codec", "lzf"))


  val configurationsSet6_11 = HashMap[String, String](("spark.reducer.maxSizeInFlight", "24m"), ("spark.rdd.compress", "true"))


  val configurationsSet6_12 = HashMap[String, String](("spark.reducer.maxSizeInFlight", "24m"), ("spark.shuffle.io.preferDirectBufs", "false"))

  val configurationsSet6_13 = HashMap[String, String](("spark.reducer.maxSizeInFlight", "24m"), ("spark.shuffle.spill.compress", "false"))


  //====== spark.shuffle.file.buffer", "48k row =====//






  val configurationsSet7_9 = HashMap[String, String](("spark.shuffle.file.buffer", "48k"), ("spark.shuffle.compress", "false"))


  val configurationsSet7_10 = HashMap[String, String](("spark.shuffle.file.buffer", "48k"), ("spark.io.compression.codec", "lzf"))


  val configurationsSet7_11 = HashMap[String, String](("spark.shuffle.file.buffer", "48k"), ("spark.rdd.compress", "true"))


  val configurationsSet7_12 = HashMap[String, String](("spark.shuffle.file.buffer", "48k"), ("spark.shuffle.io.preferDirectBufs", "false"))


  val configurationsSet7_13 = HashMap[String, String](("spark.shuffle.file.buffer", "48k"), ("spark.shuffle.spill.compress", "false"))




  //====== spark.shuffle.file.buffer", 16k row =====//





  val configurationsSet8_9 = HashMap[String, String](("spark.shuffle.file.buffer", "16k"), ("spark.shuffle.compress", "false"))


  val configurationsSet8_10 = HashMap[String, String](("spark.shuffle.file.buffer", "16k"), ("spark.io.compression.codec", "lzf"))


  val configurationsSet8_11 = HashMap[String, String](("spark.shuffle.file.buffer", "16k"), ("spark.rdd.compress", "true"))


  val configurationsSet8_12 = HashMap[String, String](("spark.shuffle.file.buffer", "16k"), ("spark.shuffle.io.preferDirectBufs", "false"))


  val configurationsSet8_13 = HashMap[String, String](("spark.shuffle.file.buffer", "16k"), ("spark.shuffle.spill.compress", "false"))


  //===== spark.shuffle.compress: false row ===//



  val configurationsSet9_10 = HashMap[String, String](("spark.shuffle.compress", "false"), ("spark.io.compression.codec", "lzf"))

  val configurationsSet9_11 = HashMap[String, String](("spark.shuffle.compress", "false"), ("spark.rdd.compress", "true"))

  val configurationsSet9_12 = HashMap[String, String](("spark.shuffle.compress", "false"), ("spark.shuffle.io.preferDirectBufs", "false"))

  val configurationsSet9_13 = HashMap[String, String](("spark.shuffle.compress", "false"), ("spark.shuffle.spill.compress", "false"))


  //==== spark.io.compression.codec: lzf row ==//


  val configurationsSet10_11 = HashMap[String, String](("spark.io.compression.codec", "lzf"), ("spark.rdd.compress", "true"))

  val configurationsSet10_12 = HashMap[String, String](("spark.io.compression.codec", "lzf"), ("spark.shuffle.io.preferDirectBufs", "false"))

  val configurationsSet10_13 = HashMap[String, String](("spark.io.compression.codec", "lzf"), ("spark.shuffle.spill.compress", "false"))



  //==== spark.rdd.compress: true row ====\\



  val configurationsSet11_12 = HashMap[String, String](("spark.rdd.compress", "true"), ("spark.shuffle.io.preferDirectBufs", "false"))

  val configurationsSet11_13 = HashMap[String, String](("spark.rdd.compress", "true"), ("spark.shuffle.spill.compress", "false"))


  //=== spark.shuffle.io.preferDirectBufs: false row ==\\


  val configurationsSet12_13 = HashMap[String, String](("spark.shuffle.io.preferDirectBufs", "false"), ("spark.shuffle.spill.compress", "false"))



  //=== All configuration pairs ==\\

  val allConfigurationsSet = Array(configurationsSet1_2, configurationsSet1_3,configurationsSet1_4, configurationsSet1_5, configurationsSet1_6, configurationsSet1_7, configurationsSet1_8, configurationsSet1_9, configurationsSet1_10, configurationsSet1_11, configurationsSet1_12, configurationsSet1_13,
    configurationsSet2_5, configurationsSet2_6, configurationsSet2_7, configurationsSet2_8, configurationsSet2_9, configurationsSet2_10, configurationsSet2_11, configurationsSet2_12, configurationsSet2_13,
    configurationsSet3_5, configurationsSet3_6, configurationsSet3_7, configurationsSet3_8, configurationsSet3_9, configurationsSet3_10, configurationsSet3_11, configurationsSet3_12, configurationsSet3_13,
    configurationsSet4_5, configurationsSet4_6, configurationsSet4_7, configurationsSet4_8, configurationsSet4_9, configurationsSet4_10, configurationsSet4_11, configurationsSet4_12, configurationsSet4_13,
    configurationsSet5_7, configurationsSet5_8, configurationsSet5_9, configurationsSet5_10, configurationsSet5_11, configurationsSet5_12, configurationsSet5_13,
    configurationsSet6_7, configurationsSet6_8, configurationsSet6_9, configurationsSet6_10, configurationsSet6_11, configurationsSet6_12, configurationsSet6_13,
    configurationsSet7_9, configurationsSet7_10, configurationsSet7_11, configurationsSet7_12, configurationsSet7_13,
    configurationsSet8_9, configurationsSet8_10, configurationsSet8_11, configurationsSet8_12, configurationsSet8_13,
    configurationsSet9_10, configurationsSet9_11, configurationsSet9_12, configurationsSet9_13,
    configurationsSet10_11, configurationsSet10_12, configurationsSet10_13,
    configurationsSet11_12, configurationsSet11_13,
    configurationsSet12_13)

  val bestConfigurationSystematic = HashMap[String, String](("spark.shuffle.file.buffer", "16k"), ("spark.shuffle.compress", "false"))

  val bestConfigurationIterative = HashMap[String, String](("spark.serializer", "org.apache.spark.serializer.KryoSerializer"), ("spark.memory.storageFraction", "0.4"))

  var numOfConfigurations = allConfigurationsSet.length
  var numOfTries = 5
  var tries = 0
  var times= Array.ofDim[Double](numOfConfigurations, numOfTries)
  var median: Array[Double] = new Array[Double](numOfConfigurations)
  var avg: Array[Double] = new Array[Double](numOfConfigurations)

  var i = 0
  for (configurationsSet <- allConfigurationsSet) {
    tries = 0
    while(tries < numOfTries) {


      val startJobTime = System.nanoTime()
      val PATH = "/home/velisarios/Random Files/SearchEngineData/TestData/Lisa2/"
      val conf = new SparkConf().setAppName(s"LDAExample").setMaster("local[*]").setAll(configurationsSet)
      val spark = SparkSession.builder().config(conf).getOrCreate()
      val sc = spark.sparkContext
      val lda = new LDAExample(sc, spark)
      val defaultParams = Params().copy(input = PATH)
      lda.run(defaultParams)

      val endJobTime = (System.nanoTime() - startJobTime) / 1e9d

      println("Whole job took " + endJobTime + "s")
      times(i)(tries) = endJobTime
      spark.stop()
      tries = tries + 1
    }
   i = i + 1

   }

  // FileWriter
  val file = new File("LDAResults.txt")
  val bw = new BufferedWriter(new FileWriter(file,true))

  //== 10g FILE ==\\
  median = LDAHelper.calculateMedian(times)
  avg = LDAHelper.calculateAvg(times)
  var j = 0
  for (time <- times) {
    bw.write("Conf " + allConfigurationsSet(j) + " job" + "\n")
    for(p <- 0 to time.length -1){
      bw.write("time " + p + " = " + time(p) + "\n")
    }
    bw.write("Conf " + j + " median:  " + median(j) + "\n")
    bw.write("Conf " + j + " avg:  " + avg(j) + "\n" )

    j = j + 1

    bw.write("\n\n")
  }
  bw.close()


}

object LDAHelper {

  def calculateMedian(times : Array[Array[Double]]):Array[Double] = {

    for(values <- times){
      scala.util.Sorting.quickSort(values)
    }

    var medianOfTimes: Array[Double] = new Array[Double](times.length)


    var i = 0
    for(values <- times){
      if(values.length % 2 ==0){
        medianOfTimes(i) = values(values.length/2)
      }else{
        medianOfTimes(i) = (values((values.length-1)/2) + values(values.length/2)) / 2.0
      }

      i = i + 1

    }


    return medianOfTimes
  }

  def calculateAvg(times : Array[Array[Double]]):Array[Double] = {
    var avgOfTimes: Array[Double] = new Array[Double](times.length)
    for(i <- 0 to avgOfTimes.length - 1){
      avgOfTimes(i) = 0
    }

    var i = 0
    for(values <- times) {
      for(k <- 0 to  values.length - 1){
        avgOfTimes(i) = avgOfTimes(i) + times(i)(k)
      }

      avgOfTimes(i) = avgOfTimes(i) / values.length

      i = i + 1
    }

    return avgOfTimes
  }

  def filterSpecialCharacters(document: String) = document.replaceAll("""[! @ # $ % ^ & * ( ) _ + - − , " ' ; : . ` ? --]""", " ")

  def getStemmedText(document: String) = {
    val morphology = new Morphology()
    new Document(document).sentences().toList.flatMap(_.words().toList.map(morphology.stem)).mkString(" ")
  }

  def getLemmaText(document: String, morphology: Morphology) = {
    val string = new StringBuilder()
    val value = new Document(document).sentences().toList.flatMap { a =>
      val words = a.words().toList
      val tags = a.posTags().toList
      (words zip tags).toMap.map { a =>
        val newWord = morphology.lemma(a._1, a._2)
        val addedWoed = if (newWord.length > 3) {
          newWord
        } else {
          ""
        }
        string.append(addedWoed + " ")
      }
    }
    string.toString()
  }
}