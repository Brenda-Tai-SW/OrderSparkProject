package order
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class OrdersProcessor {

 val  goldenLast30Days="C:/order_file/golden/Last30Days"
  val goldenLast24Month="C:/order_file/golden/Last24Month"

  def computeDailySalesTrendLast30Days(deduplicatedDataDF: DataFrame): Unit = {

    val last30DaysDF = deduplicatedDataDF
        .filter(col("date") >= date_sub(current_date(), 30))
        .groupBy("date")
        .agg(sum("total amount").as("total_sales"))
        .orderBy("date")

        last30DaysDF.write
          .mode(SaveMode.Overwrite)
          .option("header", "true")
       .   csv(goldenLast30Days)
  }


  def computeMonthlySalesTrendLast24(deduplicatedDataDF: DataFrame): Unit= {

    val Last24Month = deduplicatedDataDF
       .withColumn("year_month", date_format(col("date"), "yyyy-MM"))


    val currentYearMonth = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM"))
    val twentyFourMonthsAgo = LocalDate.now().minusMonths(23).format(DateTimeFormatter.ofPattern("yyyy-MM"))

    val filterLast24MonthDF = Last24Month
      .filter(col("year_month") >= twentyFourMonthsAgo && col("year_month") < currentYearMonth)

    // Step 4: Aggregate sales data
    val salesByMonthDF = filterLast24MonthDF
      .groupBy("year_month")
      .agg(sum("total amount").alias("monthly_sales"))
      .orderBy("year_month")

    salesByMonthDF.write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv(goldenLast24Month)



  }

}
