import org.apache.spark.SparkContext


object WordCountPractice extends App
{
  val sc = new SparkContext("local[*]","wordcountpractice")  
  
  val baseRdd = sc.textFile("file:///C:/Users/KIIT/Desktop/DATA/wordcount-data.txt")
  
  val rdd1 = baseRdd.flatMap(x => x.split(" "))
  
  val rdd2 = rdd1.map( x => (x.toLowerCase(),1))
  
  val rdd3 = rdd2.reduceByKey( (x,y) => (x+y))
  
  val rdd4 = rdd3.sortBy(x => (x._1,false))
  
  rdd4.collect.foreach(println)
}

//Equivalent Pyspark code 
/*

from pyspark import SparkContext

sc = SparkContext("local[*]","wordcount")

base_Rdd = sc.textFile("file:///C:/Users/KIIT/Desktop/DATA/wordcount-data.txt")

rdd1 = base_Rdd.flatMap( lambda x : x.split(" "))

mapped_rdd = rdd1.map( lambda x : (x[0].lower(),1))

final_rdd = mapped_rdd.reduceByKey( lambda x,y : (x+y))

result = final_rdd.collect()

for i in result:
    print(i)
    
    */
