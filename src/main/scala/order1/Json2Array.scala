package order1

import net.liftweb.json.{DefaultFormats, parse}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Json2Array extends App {

  lazy implicit val formats: DefaultFormats = net.liftweb.json.DefaultFormats
  /**
   * 1.
   * {
   * "dealDnaId" : "19093009iYQ9JLD" , "keyDatesInfo" :
   * [{ "crmDt" : "" , "dealDateType" : "RFP" , "dealDt" : "2019-12-20" }, { "crmDt" : "" , "dealDateType" :
   * "SSM Engagement" , "dealDt" : "2019-10-01" }, { "crmDt" : "" , "dealDateType" : "Solution Release" , "dealDt" :
   * "2020-02-22" }, { "crmDt" : "" , "dealDateType" : "Solution Review" , "dealDt" : "" }, { "crmDt" : "" ,
   * "dealDateType" : "Price Release" , "dealDt" : "2019-10-29" }, { "crmDt" : "" , "dealDateType" :
   * "Bid Submission" , "dealDt" : "2020-01-25" }, { "crmDt" : "2019-10-02" , "dealDateType" : "Decision" , "dealDt"
   * : "" }, { "crmDt" : "2019-10-02" , "dealDateType" : "Delivery Start" , "dealDt" : "" }, { "crmDt" : "" ,
   * "dealDateType" : "Test Date" , "dealDt" : "2020-02-01" }, { "crmDt" : "" , "dealDateType" : "Test Date 2" ,
   * "dealDt" : "2020-02-02" }, { "crmDt" : "" , "dealDateType" : "Decision Date" , "dealDt" : "2020-01-21" }] ,
   * "statusCd" : "Completed"
   * }
   */

  // Create Session
  val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("Hello-World")
    .getOrCreate()

  // Import implicits

  import spark.implicits._

  /* Do not remove this line */
  implicit val sc: SparkContext = spark.sparkContext

  // Your code start here:

  case class Data
  (
    dealDnaId: String,
    keyDatesInfo: Seq[KeyDatesInfo]
  )

  case class KeyDatesInfo
  (
    dealDateType: Option[String],
    dealDt: Option[String],
    crmDt: Option[String]

  )

  case class FinalData
  (
    dealDnaId: String,
//    dealDateType:Option[String],
//    dealDt: Option[String],
//    crmDt: Option[String],
    deal_Date_Type: String,
    deal_Dt: String,
    crm_Dt: String
  )

  //  Either[B, A]; 可能为A，也可能为B，但默认是A
  //  Try; 代表可能出错的结果 -> Either[Throwable, A]
  //  Option; 代表有可能有，有可能没有，安全的空对象 -> Either[Null, A]
  //  Future; 代表这个值现在没有，未来可能有，也可能出错 -> Option[Either[Throwable, A]] + 异步


  """{"dealDnaId": "21052010povIFBw",
    |"keyDatesInfo": [{"dealDateType": "Bid Submission", "dealDt": "2021-05-25"}]
    |}""".stripMargin

  val json =
    """{
      |    "dealDnaId": "19093009iYQ9JLD",
      |    "keyDatesInfo":[
      |    {
      |      "crmDt": ""
      |      , "dealDateType": "dd/mm/yy"
      |      , "dealDt": ""
      |    }
      |    ,
      |    {
      |      "crmDt": ""
      |      , "dealDateType": "SSM Engagement"
      |      , "dealDt": "2019-10-01"
      |    }
      |    ,
      |    {
      |      "crmDt": ""
      |      , "dealDateType": "Solution Release"
      |      , "dealDt": "2020-02-22"
      |    }
      |    ,
      |    {
      |      "crmDt": ""
      |      , "dealDateType": "Solution Review"
      |      , "dealDt": ""
      |    }
      |    ,
      |    {
      |      "crmDt": ""
      |      , "dealDateType": "Price Release"
      |      , "dealDt": "2019-10-29"
      |    }
      |    ,
      |    {
      |      "crmDt": ""
      |      , "dealDateType": "Bid Submission"
      |      , "dealDt": "2020-01-25"
      |    }
      |    ,
      |    {
      |      "crmDt": "2019-10-02"
      |      , "dealDateType": "Decision"
      |      , "dealDt": ""
      |    }
      |    ,
      |    {
      |      "crmDt": "2019-10-02"
      |      , "dealDateType": ""
      |      , "dealDt": ""
      |    }
      |    ,
      |    {
      |      "crmDt": ""
      |      , "dealDateType": "Test Date"
      |      , "dealDt": "2020-02-01"
      |    }
      |    ,
      |    {
      |      "crmDt": ""
      |      , "dealDateType": "Test Date 2"
      |      , "dealDt": "2020-02-02"
      |    }
      |    ,
      |    {
      |      "crmDt": ""
      |      , "dealDateType": "Decision Date"
      |      , "dealDt": "2020-01-21"
      |    }
      |    ], "statusCd": "Completed"}""".stripMargin



  val data: Data = parse(json).extract[Data]

//    data.keyDatesInfo.map(it => it.crmDt match {
//      case Some(value) => if (value.trim.nonEmpty) it else it.copy(crmDt = None)
//      case None => it
//    })
//      .toList.toDF()
//      .withColumn("dealDt", udf(
//        (str: String) => if (str.trim.isEmpty) null else str
//      ).apply(col("dealDt")))
//      .withColumn("dealDnaId", lit(data.dealDnaId)).show()

  val list = data.keyDatesInfo
    .map(it => it.dealDateType match {
      case Some(value) => if (value.trim.nonEmpty) it else it.copy(dealDateType = None)
      case None => it
    })
    .map(it => it.crmDt match {
      case Some(value) => if (value.trim.nonEmpty) it else it.copy(crmDt = None)
      case None => it
    })
    .map(it => it.dealDt match {
      case Some(value) => if (value.trim.nonEmpty) it else it.copy(dealDt = None)
      case None => it
    })
    .map {
      case KeyDatesInfo(dealDateType, dealDt, crmDt) =>
        FinalData(
          dealDnaId = data.dealDnaId,
//          dealDateType = dealDateType,
//          dealDt = dealDt,
//          crmDt = crmDt,
          deal_Date_Type =dealDateType.getOrElse(""),
          deal_Dt =dealDt.getOrElse(""),
          crm_Dt = crmDt.getOrElse("")
        )
    }.toList


  println("==list ===")



  //List(Q2cHubInitiateData(21061612Fb6B2aC,Solution Review,Some(2021-06-28),None))

  //List(FinalData(19093009iYQ9JLD,RFP,Some(2019-12-20),None), FinalData(19093009iYQ9JLD,SSM Engagement,Some(2019-10-01),None), FinalData(19093009iYQ9JLD,Solution Release,Some(2020-02-22),None), FinalData(19093009iYQ9JLD,Solution Review,None,None), FinalData(19093009iYQ9JLD,Price Release,Some(2019-10-29),None), FinalData(19093009iYQ9JLD,Bid Submission,Some(2020-01-25),None), FinalData(19093009iYQ9JLD,Decision,None,Some(2019-10-02)), FinalData(19093009iYQ9JLD,Delivery Start,None,Some(2019-10-02)), FinalData(19093009iYQ9JLD,Test Date,Some(2020-02-01),None), FinalData(19093009iYQ9JLD,Test Date 2,Some(2020-02-02),None), FinalData(19093009iYQ9JLD,Decision Date,Some(2020-01-21),None))
  //
  val df = Seq(("A1", "B1")).toDF("A", "B")

  val fun = udf((str: String) => list)

  println("==fun ===")
  println(fun)

  val df1 = df
    .withColumn("temp", explode(fun.apply(col("B"))))
//    .select("A", "B", "temp.dealDateType", "temp.dealDt", "temp.crmDt","temp.crm_Dt","temp.deal_Dt")

    .select("A", "B", "temp.deal_Date_Type", "temp.crm_Dt","temp.deal_Dt")

  df1.show()

  val dealDateTypeCode_isValid = Set("RFP",
    "SSM Engagement",
    "Solution Release",
    "Solution Review",
    "Price Release",
    "Bid Submission",
    "Close",
    "Delivery Start")

  val preProducerDF= df1.filter(row=>dealDateTypeCode_isValid.contains(row.getAs("deal_Date_Type"))).show()

  val df2 =df1.filter(col("deal_Date_Type")==="RFP"
    ||col("deal_Date_Type") === "SSM Engagement"
    ||col("deal_Date_Type")=== "Solution Release"
    ||col("deal_Date_Type")  === "Solution Review"
    ||col("deal_Date_Type")=== "Price Release"
    ||col("deal_Date_Type")=== "Bid Submission"
    ||col("deal_Date_Type")=== "Close"
    ||col("deal_Date_Type") === "Delivery Start").show()

}
