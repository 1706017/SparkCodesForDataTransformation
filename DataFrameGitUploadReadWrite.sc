DATASET : orderspractice.csv

orderid,orderdate,customerid,orderstatus
1,"12-01-2019",123,"COMPLTED"
2,"12-02-2019",456,"PENDING"
3,"12-02-2019",456,"CLOSED"
4,"13-04-2022",345,"COMPLETED"

Requirement :
 1) to create boiler plate code for spark (logger and spark session creation and stop)
 2) read the file and creating dataframe from the file 
 3) Partiotion based on orderstatus
 5) drop the records on basis of orderdate and customerid
 6) drop orderid
 7) write this data into a file using spark writer api and that is partition by orderstatus
 
 
 
 import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.parquet.format.DateType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SaveMode
import org.apache.log4j.Logger


object DataFrameGitUploadReadWrite extends App{
  
  //creating config for SparkSession
  val sparkConf = new SparkConf
  sparkConf.set("spark.appName","Git Hub Upload app")
  sparkConf.set("spark.master","local[2]")
  
  //creating Spark session and passing config to this
  val spark = SparkSession.builder
  .config(sparkConf)
  .getOrCreate()
  
  val ordersDf = spark.read
  .format("csv")
  .option("inferSchema",true)
  .option("header",true)
  .option("path","file:///C:/Users/KIIT/Desktop/gitHubUpload_data/orders.csv")
  .load()
  
  import spark.implicits._
  
  val ordersNewDf = ordersDf
  .withColumn("newid",monotonically_increasing_id)
  .dropDuplicates("orderdate","customerid")
  .drop("orderid")
  .sort("orderdate")
  
  ordersDf.printSchema()
  
  ordersDf.show()
  
  ordersNewDf.write
  .format("csv")
  .partitionBy("orderstatus")
  .mode(SaveMode.Overwrite)
  .option("path","file:///C:/Users/KIIT/Desktop/gitHubUpload_data/newoutputfolder1")
  .save()
  
  
   Logger.getLogger(getClass.getName).info("Succesfully written data ")
  
  
  //to stop the sparkSession
  spark.stop()
  
}
