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
