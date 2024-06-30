package order
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class OrdersProcessor {

  def computeDailySalesTrendLast30Days(deduplicatedDataDF: DataFrame): DataFrame = {

    deduplicatedDataDF.show()

         deduplicatedDataDF
        .filter(col("date") >= date_sub(current_date(), 30))
        .groupBy("date")
        .agg(sum("total amount").as("total_sales"))
        .orderBy("date")

  }


  def computeMonthlySalesTrendLast24(deduplicatedDataDF: DataFrame): DataFrame= {

    val Last24Month = deduplicatedDataDF
       .withColumn("year_month", date_format(col("date"), "yyyy-MM"))


    val currentYearMonth = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM"))
    val twentyFourMonthsAgo = LocalDate.now().minusMonths(23).format(DateTimeFormatter.ofPattern("yyyy-MM"))

    val filterLast24MonthDF = Last24Month
      .filter(col("year_month") >= twentyFourMonthsAgo && col("year_month") < currentYearMonth)

      filterLast24MonthDF
      .groupBy("year_month")
      .agg(sum("total amount").alias("monthly_sales"))
      .orderBy("year_month")

}

}
