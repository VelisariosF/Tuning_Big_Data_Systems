val PATH_TO_10GB_FILE = "/home/velisarios/AUTH/DiplomaProject/BenchmarkTest/Sort_By_Key/sortByKeyTestFile10gb.txt"
val PATH_TO_1GB_FILE = "/home/velisarios/AUTH/DiplomaProject/BenchmarkTest/Sort_By_Key/sortByKeyTestFile1gb.txt"

//// 10 gb file //////
val jobStart = System.nanoTime()



val lines = sc.textFile(PATH_TO_10GB_FILE).map(s => {
            val substrings = s.split(":")
            (substrings(0), substrings(1))
          })

val startCalcTime = System.nanoTime()
lines.groupByKey()
.take(10)
val endCalcTime = (System.nanoTime() - startCalcTime) / 1e9d


val jobEnd = (System.nanoTime() - jobStart) / 1e9d

print("The (10gb) calculations took " + endCalcTime + "s")
println("Whole (10gb) job took " + jobEnd + "s")




 //// 1 gb file //////

val jobStart2 = System.nanoTime()



val lines = sc.textFile(PATH_TO_1GB_FILE).map(s => {
            val substrings = s.split(":")
            (substrings(0), substrings(1))
          })

val startCalcTime2 = System.nanoTime()
lines.groupByKey()
.take(10)
val endCalcTime2 = (System.nanoTime() - startCalcTime2) / 1e9d


val jobEnd2 = (System.nanoTime() - jobStart2) / 1e9d

print("The (1gb) calculations took " + endCalcTime2 + "s")
println("Whole (1gb) job took " + jobEnd2 + "s")
