package order

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession,Row}
import org.apache.spark.sql.functions._
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Properties
import javax.mail.{Message, Session, Transport}
import javax.mail.internet.{InternetAddress, MimeMessage}
import org.quartz.Job
import org.quartz.JobExecutionContext
import org.quartz.{JobBuilder, JobDetail, Scheduler, SchedulerException, Trigger, TriggerBuilder}
import org.quartz.SchedulerFactory
import org.quartz.impl.StdSchedulerFactory
import org.quartz.CronScheduleBuilder.cronSchedule

class EmailProcessor {

  def getLastCalendarMonth: (String, String) = {
    val now = LocalDate.now
    val firstDayOfCurrentMonth = now.withDayOfMonth(1)
    val lastDayOfLastMonth = firstDayOfCurrentMonth.minusDays(1)
    val firstDayOfLastMonth = lastDayOfLastMonth.withDayOfMonth(1)

    val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    (
      firstDayOfLastMonth.format(dateFormatter),
      lastDayOfLastMonth.format(dateFormatter)
    )
  }

  def prepareCustomerEmailData(ordersDF: DataFrame): DataFrame = {
    val (startDate, endDate) = getLastCalendarMonth

    val lastMonthOrdersDF = ordersDF.filter(
      col("date").between(startDate, endDate)
    )

    val summaryDF = lastMonthOrdersDF.groupBy("customer")
      .agg(
        count("order_no").as("number_of_orders"),
        sum("total amount").as("monthly_total_amount")
      )

    summaryDF.join(lastMonthOrdersDF, "customer")
  }

  def sendEmails(ordersDF: DataFrame): Unit = {
    val emailDataDF = prepareCustomerEmailData(ordersDF)

    val customerGroups = emailDataDF.groupBy("customer").agg(
      collect_list(struct(
        col("order_no"),
        col("productId"),
        col("unit price"),
        col("quantity"),
        col("total amount"),
        col("date")
      )).as("orders"),
      first("number_of_orders").as("number_of_orders"),
      first("monthly_total_amount").as("monthly_total_amount")
    )

    customerGroups.collect().foreach { row =>
      val customer = row.getAs[String]("customer")
      val numberOfOrders = row.getAs[Long]("number_of_orders")
      val monthTotalAmount = row.getAs[Double]("monthly_total_amount")
      val orders = row.getAs[Seq[Row]]("orders")

      val emailContent = buildEmailContent(customer, numberOfOrders, monthTotalAmount, orders)
      sendEmail(customer, emailContent)
    }
  }

  def buildEmailContent(customer: String, numberOfOrders: Long, monthlyTotalAmount: Double, orders: Seq[Row]): String = {
    val summary = s"Monthly Summary for Customer $customer:\n" +
      s"Number of Orders: $numberOfOrders\n" +
      s"Monthly Total Amount: ${monthlyTotalAmount}\n\n"

    val orderDetails = orders.sortBy(_.getAs[String]("date")).map { order =>
      val orderNo = order.getAs[String]("order_no")
      val orderDate = order.getAs[String]("date")
      val productId = order.getAs[String]("productId")
      val unitPrice = convertToDouble(order.getAs[Any]("unit price"))
      val quantity = convertToInt(order.getAs[Any]("quantity"))
      val totalAmount = convertToDouble(order.getAs[Any]("total amount"))
      s"Order No: $orderNo, Order Date: $orderDate, Product ID: $productId, Quantity: $quantity,Unit Price: $unitPrice, Total Amount: ${totalAmount}"
    }.mkString("\n")

    summary + "Order Details:\n" + orderDetails
  }

  def sendEmail(customerEmail: String, emailContent: String): Unit = {
    val fromEmail = "Brenda.Tai101.Sweden@gmail.com"
    val fromPassword = "exclxfmudrzndstq"

    val props = new Properties()
    props.put("mail.smtp.auth", "true")
    props.put("mail.smtp.starttls.enable", "true")
    props.put("mail.smtp.host", "smtp.gmail.com")
    props.put("mail.smtp.port", "587")

    val session = Session.getInstance(props, new javax.mail.Authenticator() {
      protected override def getPasswordAuthentication = {
        new javax.mail.PasswordAuthentication(fromEmail, fromPassword)
      }
    })

    try {
      val message = new MimeMessage(session)
      message.setFrom(new InternetAddress(fromEmail))
      message.setRecipient(Message.RecipientType.TO, InternetAddress.parse("Brenda.Tai101.Sweden@gmail.com").head)
      message.setSubject("Monthly Order Summary")
      message.setText(emailContent)

      Transport.send(message)
      println(s"Email sent successfully to $customerEmail")
    } catch {
      case e: Exception =>
        e.printStackTrace()
        println(s"Failed to send email to $customerEmail")
    }
  }


  def convertToDouble(value: Any): Double = {
    value match {
      case null => 0.0
      case value: Double => value
      case value: String => value.toDouble
      case _ => throw new IllegalArgumentException("Unexpected value type")
    }
  }


  def convertToInt(value: Any): Int = {
    value match {
      case null => 0
      case value: Int => value
      case value: Double => value.toInt
      case value: String => value.toInt
      case _ => throw new IllegalArgumentException("Unexpected value type")
    }

  }

  def scheduleEmailJob(ordersDF: DataFrame): Unit = {
    val schedulerFactory: SchedulerFactory = new StdSchedulerFactory()
    val scheduler: Scheduler = schedulerFactory.getScheduler
    scheduler.start()

    val jobDetail: JobDetail = JobBuilder.newJob(classOf[EmailJob])
      .withIdentity("emailJob", "group1")
      .build()

    jobDetail.getJobDataMap.put("ordersDF", ordersDF)

    val trigger: Trigger = TriggerBuilder.newTrigger()
      .withIdentity("trigger1", "group1")
      //.withSchedule(cronSchedule("0 0 9 1 * ?"))  // Triggered at 9am on the 1st of the month
      .withSchedule(cronSchedule("0 55 17 ? * *"))
      .forJob("emailJob", "group1")
      .build()

    scheduler.scheduleJob(jobDetail, trigger)
   // scheduler.triggerJob(jobDetail.getKey)
  }

}


class EmailJob extends Job {
  def execute(context: JobExecutionContext): Unit = {
    val ordersDF = context.getMergedJobDataMap.get("ordersDF").asInstanceOf[DataFrame]

    println(s"Sending email to customer -----EmailJob111")

    val emailProcessor = new EmailProcessor()
        emailProcessor.sendEmails(ordersDF)

    println(s" after Sending email to customer -----EmailJob222")
  }
}