import org.apache.spark.SparkContext


object ValuableCustomers extends App
{
  val sc = new SparkContext("local[*]","valuablecustomers")  
  
  val baseRdd = sc.textFile("file:///C:/Users/KIIT/Desktop/DATA/customerAmountData.txt")
  
  val mappedRdd = baseRdd.map(x => (x.split(",")(0),x.split(",")(2).toFloat))
  
  val totalAmount = mappedRdd.reduceByKey( (x,y) => (x+y))
  
  val sortedAmount = totalAmount.sortByKey(false) //Here false means descending order
  
  sortedAmount.collect.foreach(println)
}



//Equivalent Pyspark code

/*

from pyspark import SparkContext

sc = SparkContext("local[*]","wordcount")

base_Rdd = sc.textFile("file:///C:/Users/KIIT/Desktop/DATA/wordcount-data.txt")

extracted_rdd = base_Rdd.map(lambda x : (x.split(",")[0],float(x.split(",")[2])))

total_Amount = extracted_rdd.reduceByKey( lambda x,y :(x+y))

sorted = total_Amount.sortByKey().False

result =sorted.collect()

for i in result:
    print(i)



*/
