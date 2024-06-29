package order
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

class ProductsProcessor {

  val  top10MostSteadilySold="C:/order_file/golden/top10MostSteadilySold"

  def computeTopSteadyProducts(deduplicatedDataDF: DataFrame): Unit = {
    val recent6MonthsDF = deduplicatedDataDF
      .filter(col("date") >= date_sub(current_date(), 180))

    // Aggregate sales data by product and month
    val salesByProductMonthDF = recent6MonthsDF
      .withColumn("year_month", date_format(col("date"), "yyyy-MM"))
      .groupBy("productId", "year_month")
      .agg(sum("quantity").as("total_units_sold"))

    val productSalesStatsDF = salesByProductMonthDF
      .groupBy("productId")
      .agg(
        stddev("total_units_sold").as("sales_stddev"),
        avg("total_units_sold").as("average_units_sold")
      )

    val rankedProductsDF = productSalesStatsDF
      .orderBy("sales_stddev", "average_units_sold")
      .limit(10)

    rankedProductsDF.write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv(top10MostSteadilySold)
  }

}
