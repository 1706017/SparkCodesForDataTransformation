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
