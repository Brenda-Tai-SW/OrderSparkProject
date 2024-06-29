package order

import net.liftweb.json.JsonDSL._
import net.liftweb.json.{DefaultFormats, parse}
import net.liftweb.json.Extraction._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}

object OrdersMainApp extends App {
  // Create Session
  val spark = SparkSession.builder()
    .appName("order main app")
    .master("local[*]")
    .getOrCreate()

  val sourceFilePath = "C:/order_file/sourceFile/order_list-1(1).csv"
  val bronzeFilePath = "C:/order_file/bronze"
  val sliverFilePath = "C:/order_file/sliver"


  val sourceDataDF = spark.read.format("csv")
    .option("header", "true")
    .load(sourceFilePath)

  sourceDataDF.write
    .mode(SaveMode.Overwrite)
    .option("header", "true")
    .csv(bronzeFilePath)



  val removeDuplicatedDataDF = sourceDataDF.dropDuplicates()


   removeDuplicatedDataDF.show()

  removeDuplicatedDataDF.write
    .mode(SaveMode.Overwrite)
    .option("header", "true")
    .csv(sliverFilePath)


  val ordersProcessor = new OrdersProcessor()
   ordersProcessor.computeDailySalesTrendLast30Days(removeDuplicatedDataDF)
   ordersProcessor.computeMonthlySalesTrendLast24(removeDuplicatedDataDF)

  val productsProcessor = new ProductsProcessor()
  productsProcessor.computeTopSteadyProducts(removeDuplicatedDataDF)

  val emailProcessor = new EmailProcessor()
   emailProcessor.sendEmails(sourceDataDF)
    emailProcessor.scheduleEmailJob(sourceDataDF)




}
