import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode


object DataFrameReadWriteAndLoadIntoTable extends App
{
  val sparkConf = new SparkConf
  sparkConf.set("spark.app.Name","Spark Application to Read Process and Write/Load Data into Table")
  sparkConf.set("spark.master","local[2]")
  
  val spark =  SparkSession.builder
               .config(sparkConf)
               .enableHiveSupport() //This will tell that to enable hive so that we can use hive metastore for meta data of the table 
               .getOrCreate()
  
  //Here we are defining the schema to our data set
  val ordersSchema = "Order_Id Int,Order_Date Timestamp,Order_Customer_Id Int,Order_Status String"
  
  
               
  val ordersDf = spark.read
                 .format("csv") //Here if we do not give this format by default it will be parquet file format
                 .option("header",true)
                 .option("inferSchema",true)
                 .option("mode","DROPMALFORMED")
                 .schema(ordersSchema)
                 .option("path","file:///C:/Users/KIIT/Desktop/DATA/orders.csv")
                 .load()
                 
   ordersDf.printSchema()
   ordersDf.show()
   
   print("Read operation is done successfuylly and DataFrame gets created")
   
   /*ordersDf.write
   .format("csv") //Here if we do not pass any format by default it will be parquet format
   .partitionBy("Order_Status")
   .option("path","file:///C:/Users/KIIT/Desktop/DATA/newfolder2")
   .option("maxRecordsPerFile","2000")
   .mode(SaveMode.Overwrite)
   .save()*/
   
   
   
   //Here the data will be stored into a table that is orders1 and by default it will be stored into default database as we have not defined the custome data base
   //Here currently as soon as application terminates all the data inside table will be lost as for the metadata it is using catalog metastore which is a in memory but we want to persist our data into table such that even if application terminates the data is still there in the table
   //so we need to integrate hive meta store so that the data inside table can still be there when application is closed 
   //so we need to add jar search for keyword "spark hive 2.4.4 2.11"(versions are mentioned spark and scala)
   //to add jar follow the step right click on project -> build path -> configure build path -> libraries tab -> add external jar tab -> select the downloaded jar files -> apply and close
   //we also added the enableHiveSupport at the SparkSession so that session will be aware that we are integrating hive so to have meta data stored in hive meta store
   
   
   spark.sql("create database retail") //Here we are creating our own database with name retail
   
   ordersDf.write
   .format("csv")
   .mode(SaveMode.Overwrite)
   .bucketBy(4,"Order_Customer_Id")
   .saveAsTable("retail.orders")
   
   spark.catalog.listTables("retail").show() //This will list out all the tables inside the retail database
   
   
   print("Write operation is also done successfully and data gets loaded into the target directory after processing")
  
   spark.stop()
   
   /*Note: Benefit of loading data into Table is that reporting tools like tableaue and power Bi can fetch data from this but if we only write our processed Dataframe into files it cannot be fetched by power bi or tableau*/
  
  
}
