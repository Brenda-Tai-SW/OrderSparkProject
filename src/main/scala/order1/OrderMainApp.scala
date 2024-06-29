package order1

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

class OrderMainApp extends App {
  val spark = SparkSession.builder()
    .appName("order main app")
    .master("local[*]")
    .config("spark.some.config.option", "config-value")
    .getOrCreate()

  val filePath = "C:/Users/Owen/Desktop/bigDataTask/order_list-1(1).csv"
  val df = spark.read.format("csv")
    .option("header", "true")
    .load(filePath)

  df.show()

  // Set log level to WARN or INFO as needed
  spark.sparkContext.setLogLevel("WARN")

  // Call methods from other classes or objects to execute tasks
  val ordersProcessor = new OrdersProcessor()
 // ordersProcessor.computeDailySalesTrend("/path/to/gold/daily_sales_trend")
 // ordersProcessor.computeMonthlySalesTrend("/path/to/gold/monthly_sales_trend")

  //val productsProcessor = new ProductsProcessor()
 // productsProcessor.computeTopProducts("/path/to/gold/top_products")

 // val emailProcessor = new EmailProcessor()
   //   emailProcessor.prepareCustomerEmailData()


  // Stop SparkSession at the end of the application
 // spark.stop()





}
