package order


import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger

object OrdersMainApp extends App {

  private val logger: Logger = LogManager.getLogger(getClass.getName)


  val spark = SparkSession.builder()
    .appName("order main app")
    .master("local[*]")
    .getOrCreate()

  val storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=taimystorage;AccountKey=07iv3SX3mUW2NLk+OKywLFR88dTzhJf/zjjfriNR5aebxMyYYIBQXSabsTm5Lk0JCXXrT5ku1SAU+AStidI68Q==;EndpointSuffix=core.windows.net"
  val containerName = "order"
  val storageAccountName="taimystorage"
  val storageAccountKey= "07iv3SX3mUW2NLk+OKywLFR88dTzhJf/zjjfriNR5aebxMyYYIBQXSabsTm5Lk0JCXXrT5ku1SAU+AStidI68Q=="


  val azureStorageUploader =new AzureStorageUploader()
    azureStorageUploader.uploadFileToStorage(storageConnectionString,containerName)


  val bronzePath = s"wasbs://$containerName@$storageAccountName.blob.core.windows.net/bronze/sourceFile.csv"

  spark.conf.set(s"fs.azure.account.key.$storageAccountName.blob.core.windows.net", storageAccountKey)
  spark.conf.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")

  val bronzeDF = spark.read.option("header", "true").csv(bronzePath)

  val silverDF = bronzeDF.dropDuplicates()

   silverDF.show()

  val silverPath = s"wasbs://$containerName@$storageAccountName.blob.core.windows.net/silver"

  logger.info(s"before uplode file to $silverPath" + silverDF.count())

  silverDF.write.mode("append").option("header", "true").csv(silverPath)

  logger.info(s" after uplode file to $silverPath"+ silverDF.count())



  val ordersProcessor = new OrdersProcessor()
  val goldenLast30DaysDF= ordersProcessor.computeDailySalesTrendLast30Days(silverDF)
  val goldenLast30DaysPath = s"wasbs://$containerName@$storageAccountName.blob.core.windows.net/golden/Last30Days"


     goldenLast30DaysDF.coalesce(1).write.mode("append").option("header", "true").csv(goldenLast30DaysPath)

  val goldenLast24MonthDF= ordersProcessor.computeMonthlySalesTrendLast24(silverDF)

  val goldenLast24MonthPath = s"wasbs://$containerName@$storageAccountName.blob.core.windows.net/golden/Last24Month"
    goldenLast24MonthDF.coalesce(1).write.mode("append").option("header", "true").csv(goldenLast24MonthPath)

  val productsProcessor = new ProductsProcessor()
  val top10MostSteadilySoldDF=productsProcessor.computeTopSteadyProducts(silverDF)

  val top10MostSteadilyPath = s"wasbs://$containerName@$storageAccountName.blob.core.windows.net/golden/top10MostSteadilySold"

    top10MostSteadilySoldDF.coalesce(1).write.mode("append").option("header", "true").csv(top10MostSteadilyPath)


  val emailProcessor = new EmailProcessor()
   emailProcessor.sendEmails(silverDF)// for testing
   emailProcessor.scheduleEmailJob(silverDF)




}
