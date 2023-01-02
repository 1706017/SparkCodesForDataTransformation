import org.apache.spark.SparkContext


object MovieRatingsCalculator extends App
{
  val sc = new SparkContext("local[*]","movieratingscalculator")  
  
  val baseRdd = sc.textFile("file:///C:/Users/KIIT/Desktop/DATA/movieRatingsData.txt")
  
  val extractedRdd = baseRdd.map(x => x.split("\t")(4))
  
  val mappedRdd = extractedRdd.map( x => (x,1))
  
  val finalRdd = mappedRdd.reduceByKey( (x,y) => (x+y))
  
  val sortedRdd = finalRdd.sortByKey(false)
  
  sortedRdd.collect.foreach(println)
}

//Equivalent Pyspark code

/*

from pyspark import SparkContext

sc = SparkContext("local[*]","wordcount")

base_Rdd = sc.textFile("file:///C:/Users/KIIT/Desktop/DATA/movieRatingsData.txt")

extracted_Rdd = base_Rdd.map(lambda x : x.split("\t")[2])

mapped_Rdd = extracted_Rdd.map(lambda x : (x,1))

final_rdd = mapped_Rdd.reduceByKey( lambda x,y :(x+y))

result = final_rdd.sortByKey().False

final_result = result.collect()

for i in final_result:
    print(i)



*/
