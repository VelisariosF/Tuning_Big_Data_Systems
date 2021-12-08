package kmeans

import org.apache.spark.SparkConf
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.{OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, SparkSession}
import recommender.RunRecommender.{allConfigurationsSet, calculateAvg, calculateMedian}

import java.io.{BufferedWriter, File, FileWriter}
import scala.collection.mutable.HashMap
import scala.util.Random

object ADNT extends Serializable {

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

 // val bestConfigurationSystematic = HashMap[String, String](("spark.shuffle.file.buffer", "16k"), ("spark.shuffle.compress", "false"))

 // val bestConfigurationIterative = HashMap[String, String](("spark.serializer", "org.apache.spark.serializer.KryoSerializer"), ("spark.memory.storageFraction", "0.4"))

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
  def main(args: Array[String]): Unit = {
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
        val conf = new SparkConf().setMaster("local[*]").setAll(configurationsSet)
        val spark = SparkSession.builder()
          .appName("Spark Recommender")
          .config(conf)
          .getOrCreate()

        val data = spark.read.
          option("inferSchema", true).
          option("header", false).
          csv("/home/velisarios/hdfsFiles/kddcup_small_v3.data").
          toDF(
            "duration", "protocol_type", "service", "flag",
            "src_bytes", "dst_bytes", "land", "wrong_fragment", "urgent",
            "hot", "num_failed_logins", "logged_in", "num_compromised",
            "root_shell", "su_attempted", "num_root", "num_file_creations",
            "num_shells", "num_access_files", "num_outbound_cmds",
            "is_host_login", "is_guest_login", "count", "srv_count",
            "serror_rate", "srv_serror_rate", "rerror_rate", "srv_rerror_rate",
            "same_srv_rate", "diff_srv_rate", "srv_diff_host_rate",
            "dst_host_count", "dst_host_srv_count",
            "dst_host_same_srv_rate", "dst_host_diff_srv_rate",
            "dst_host_same_src_port_rate", "dst_host_srv_diff_host_rate",
            "dst_host_serror_rate", "dst_host_srv_serror_rate",
            "dst_host_rerror_rate", "dst_host_srv_rerror_rate",
            "label")

        data.cache()

        val runKMeans = new RunKMeans(spark)

        //   runKMeans.clusteringTake0(data)
        //   runKMeans.clusteringTake1(data)
        //   runKMeans.clusteringTake2(data)
        //   runKMeans.clusteringTake3(data)
        runKMeans.clusteringTake4(data)
        runKMeans.buildAnomalyDetector(data)

        data.unpersist()
        val endJobTime = (System.nanoTime() - startJobTime) / 1e9d

        println("Whole job " + i + "took " + endJobTime + "s")
        times(i)(tries) = endJobTime
        spark.stop()
        tries = tries + 1
      }
      i = i + 1

    }



    // FileWriter
    val file = new File("ADNTResults.txt")
    val bw = new BufferedWriter(new FileWriter(file,true))

    //== 10g FILE ==\\
    median = calculateMedian(times)
    avg = calculateAvg(times)
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

}

class RunKMeans(private val spark: SparkSession) extends Serializable {

  import spark.implicits._

  // Clustering, Take 0

  def clusteringTake0(data: DataFrame): Unit = {

    data.select("label").groupBy("label").count().orderBy($"count".desc).show(25)

    val numericOnly = data.drop("protocol_type", "service", "flag").cache()

    val assembler = new VectorAssembler().
      setInputCols(numericOnly.columns.filter(_ != "label")).
      setOutputCol("featureVector")

    val kmeans = new KMeans().
      setSeed(Random.nextLong()).
      setPredictionCol("cluster").
      setFeaturesCol("featureVector")

    val pipeline = new Pipeline().setStages(Array(assembler, kmeans))
    val pipelineModel = pipeline.fit(numericOnly)
    val kmeansModel = pipelineModel.stages.last.asInstanceOf[KMeansModel]

    kmeansModel.clusterCenters.foreach(println)

    val withCluster = pipelineModel.transform(numericOnly)

    withCluster.select("cluster", "label").
      groupBy("cluster", "label").count().
      orderBy($"cluster", $"count".desc).
      show(25)

    numericOnly.unpersist()
  }

  // Clustering, Take 1

  def clusteringScore0(data: DataFrame, k: Int): Double = {
    val assembler = new VectorAssembler().
      setInputCols(data.columns.filter(_ != "label")).
      setOutputCol("featureVector")

    val kmeans = new KMeans().
      setSeed(Random.nextLong()).
      setK(k).
      setPredictionCol("cluster").
      setFeaturesCol("featureVector")

    val pipeline = new Pipeline().setStages(Array(assembler, kmeans))

    val kmeansModel = pipeline.fit(data).stages.last.asInstanceOf[KMeansModel]
    kmeansModel.summary.trainingCost
  }

  def clusteringScore1(data: DataFrame, k: Int): Double = {
    val assembler = new VectorAssembler().
      setInputCols(data.columns.filter(_ != "label")).
      setOutputCol("featureVector")

    val kmeans = new KMeans().
      setSeed(Random.nextLong()).
      setK(k).
      setPredictionCol("cluster").
      setFeaturesCol("featureVector").
      setMaxIter(40).
      setTol(1.0e-5)

    val pipeline = new Pipeline().setStages(Array(assembler, kmeans))

    val kmeansModel = pipeline.fit(data).stages.last.asInstanceOf[KMeansModel]
    kmeansModel.summary.trainingCost
  }

  def clusteringTake1(data: DataFrame): Unit = {
    val numericOnly = data.drop("protocol_type", "service", "flag").cache()
    (20 to 100 by 20).map(k => (k, clusteringScore0(numericOnly, k))).foreach(println)
    (20 to 100 by 20).map(k => (k, clusteringScore1(numericOnly, k))).foreach(println)
    numericOnly.unpersist()
  }

  // Clustering, Take 2

  def clusteringScore2(data: DataFrame, k: Int): Double = {
    val assembler = new VectorAssembler().
      setInputCols(data.columns.filter(_ != "label")).
      setOutputCol("featureVector")

    val scaler = new StandardScaler()
      .setInputCol("featureVector")
      .setOutputCol("scaledFeatureVector")
      .setWithStd(true)
      .setWithMean(false)

    val kmeans = new KMeans().
      setSeed(Random.nextLong()).
      setK(k).
      setPredictionCol("cluster").
      setFeaturesCol("scaledFeatureVector").
      setMaxIter(40).
      setTol(1.0e-5)

    val pipeline = new Pipeline().setStages(Array(assembler, scaler, kmeans))
    val pipelineModel = pipeline.fit(data)

    val kmeansModel = pipelineModel.stages.last.asInstanceOf[KMeansModel]
    kmeansModel.summary.trainingCost
  }

  def clusteringTake2(data: DataFrame): Unit = {
    val numericOnly = data.drop("protocol_type", "service", "flag").cache()
    (60 to 270 by 30).map(k => (k, clusteringScore2(numericOnly, k))).foreach(println)
    numericOnly.unpersist()
  }

  // Clustering, Take 3

  def oneHotPipeline(inputCol: String): (Pipeline, String) = {
    val indexer = new StringIndexer().
      setInputCol(inputCol).
      setOutputCol(inputCol + "_indexed")
    val encoder = new OneHotEncoder().
      setInputCol(inputCol + "_indexed").
      setOutputCol(inputCol + "_vec")
    val pipeline = new Pipeline().setStages(Array(indexer, encoder))
    (pipeline, inputCol + "_vec")
  }

  def clusteringScore3(data: DataFrame, k: Int): Double = {
    val (protoTypeEncoder, protoTypeVecCol) = oneHotPipeline("protocol_type")
    val (serviceEncoder, serviceVecCol) = oneHotPipeline("service")
    val (flagEncoder, flagVecCol) = oneHotPipeline("flag")

    // Original columns, without label / string columns, but with new vector encoded cols
    val assembleCols = Set(data.columns: _*) --
      Seq("label", "protocol_type", "service", "flag") ++
      Seq(protoTypeVecCol, serviceVecCol, flagVecCol)
    val assembler = new VectorAssembler().
      setInputCols(assembleCols.toArray).
      setOutputCol("featureVector")

    val scaler = new StandardScaler()
      .setInputCol("featureVector")
      .setOutputCol("scaledFeatureVector")
      .setWithStd(true)
      .setWithMean(false)

    val kmeans = new KMeans().
      setSeed(Random.nextLong()).
      setK(k).
      setPredictionCol("cluster").
      setFeaturesCol("scaledFeatureVector").
      setMaxIter(40).
      setTol(1.0e-5)

    val pipeline = new Pipeline().setStages(
      Array(protoTypeEncoder, serviceEncoder, flagEncoder, assembler, scaler, kmeans))
    val pipelineModel = pipeline.fit(data)

    val kmeansModel = pipelineModel.stages.last.asInstanceOf[KMeansModel]
    kmeansModel.summary.trainingCost
  }

  def clusteringTake3(data: DataFrame): Unit = {
    (60 to 270 by 30).map(k => (k, clusteringScore3(data, k))).foreach(println)
  }

  // Clustering, Take 4

  def entropy(counts: Iterable[Int]): Double = {
    val values = counts.filter(_ > 0)
    val n = values.map(_.toDouble).sum
    values.map { v =>
      val p = v / n
      -p * math.log(p)
    }.sum
  }

  def fitPipeline4(data: DataFrame, k: Int): PipelineModel = {
    val (protoTypeEncoder, protoTypeVecCol) = oneHotPipeline("protocol_type")
    val (serviceEncoder, serviceVecCol) = oneHotPipeline("service")
    val (flagEncoder, flagVecCol) = oneHotPipeline("flag")

    // Original columns, without label / string columns, but with new vector encoded cols
    val assembleCols = Set(data.columns: _*) --
      Seq("label", "protocol_type", "service", "flag") ++
      Seq(protoTypeVecCol, serviceVecCol, flagVecCol)
    val assembler = new VectorAssembler().
      setInputCols(assembleCols.toArray).
      setOutputCol("featureVector")

    val scaler = new StandardScaler()
      .setInputCol("featureVector")
      .setOutputCol("scaledFeatureVector")
      .setWithStd(true)
      .setWithMean(false)

    val kmeans = new KMeans().
      setSeed(Random.nextLong()).
      setK(k).
      setPredictionCol("cluster").
      setFeaturesCol("scaledFeatureVector").
      setMaxIter(40).
      setTol(1.0e-5)

    val pipeline = new Pipeline().setStages(
      Array(protoTypeEncoder, serviceEncoder, flagEncoder, assembler, scaler, kmeans))
    pipeline.fit(data)
  }

  def clusteringScore4(data: DataFrame, k: Int): Double = {
    val pipelineModel = fitPipeline4(data, k)

    // Predict cluster for each datum
    val clusterLabel = pipelineModel.transform(data).
      select("cluster", "label").as[(Int, String)]
    val weightedClusterEntropy = clusterLabel.
      // Extract collections of labels, per cluster
      groupByKey { case (cluster, _) => cluster }.
      mapGroups { case (_, clusterLabels) =>
        val labels = clusterLabels.map { case (_, label) => label }.toSeq
        // Count labels in collections
        val labelCounts = labels.groupBy(identity).values.map(_.size)
        labels.size * entropy(labelCounts)
      }.collect()

    // Average entropy weighted by cluster size
    weightedClusterEntropy.sum / data.count()
  }

  def clusteringTake4(data: DataFrame): Unit = {
    (60 to 270 by 30).map(k => (k, clusteringScore4(data, k))).foreach(println)

    val pipelineModel = fitPipeline4(data, 180)
    val countByClusterLabel = pipelineModel.transform(data).
      select("cluster", "label").
      groupBy("cluster", "label").count().
      orderBy("cluster", "label")
    countByClusterLabel.show()
  }

  // Detect anomalies

  def buildAnomalyDetector(data: DataFrame): Unit = {
    val pipelineModel = fitPipeline4(data, 180)

    val kMeansModel = pipelineModel.stages.last.asInstanceOf[KMeansModel]
    val centroids = kMeansModel.clusterCenters

    val clustered = pipelineModel.transform(data)
    val threshold = clustered.
      select("cluster", "scaledFeatureVector").as[(Int, Vector)].
      map { case (cluster, vec) => Vectors.sqdist(centroids(cluster), vec) }.
      orderBy($"value".desc).take(100).last

    val originalCols = data.columns
    val anomalies = clustered.filter { row =>
      val cluster = row.getAs[Int]("cluster")
      val vec = row.getAs[Vector]("scaledFeatureVector")
      Vectors.sqdist(centroids(cluster), vec) >= threshold
    }.select(originalCols.head, originalCols.tail:_*)

    println(anomalies.first())
  }

}
