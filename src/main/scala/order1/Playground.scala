package order1

import net.liftweb.json.JsonDSL._
import net.liftweb.json.{DefaultFormats, parse}
import net.liftweb.json.Extraction._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Playground extends App {
  // Create Session
  val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("Hello-World")
    .getOrCreate()

  // Import implicits
  import spark.implicits._ /* Do not remove this line */
  implicit val sc: SparkContext = spark.sparkContext

  // Your code start here:
  val df = Seq(("Hello", "world")).toDF("greeting", "to")


  df.show()
}
