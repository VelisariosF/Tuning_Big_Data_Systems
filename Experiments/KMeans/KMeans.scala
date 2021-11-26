val PATH_TO_5GB_FILE = "/home/velisarios/AUTH/DiplomaProject/BenchmarkTest/KMeans/kmeansData5gb.csv"
val PATH_TO_1GB_FILE = "/home/velisarios/AUTH/DiplomaProject/BenchmarkTest/KMeans/kmeansData1gb.csv"


/////     5gb file ///////////////////
val startJobTime = System.nanoTime()


import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

// Load and parse the data
val data = sc.textFile(PATH_TO_5GB_FILE)
val parsedData = data.map(s => Vectors.dense(s.split(',').map(_.toDouble))).cache()

// Cluster the data into two classes using KMeans
val numClusters = 2
val numIterations = 20


val startCalcTime = System.nanoTime()
val clusters = KMeans.train(parsedData, numClusters, numIterations)

// Evaluate clustering by computing Within Set Sum of Squared Errors
val WSSSE = clusters.computeCost(parsedData)
val endCalcTime = (System.nanoTime()- startCalcTime) / 1e9d

val endJobTime = (System.nanoTime()- startJobTime) / 1e9d

 print("The (5gb) calculations took " + endCalcTime + "s")
 println("Whole (5gb) job took " + endJobTime + "s")


/////     1 gb file ///////////////////

val startJobTime2 = System.nanoTime()


import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

// Load and parse the data
val data = sc.textFile(PATH_TO_1GB_FILE)
val parsedData = data.map(s => Vectors.dense(s.split(',').map(_.toDouble))).cache()

// Cluster the data into two classes using KMeans
val numClusters = 2
val numIterations = 20
val startCalcTime2 = System.nanoTime()
val clusters = KMeans.train(parsedData, numClusters, numIterations)


// Evaluate clustering by computing Within Set Sum of Squared Errors
val WSSSE = clusters.computeCost(parsedData)
val endCalcTime2 = (System.nanoTime()- startCalcTime2) / 1e9d

val endJobTime2 = (System.nanoTime()- startJobTime2) / 1e9d

 print("The (1gb) calculations took " + endCalcTime2 + "s")
 println("Whole (1gb) job took " + endJobTime2 + "s")
