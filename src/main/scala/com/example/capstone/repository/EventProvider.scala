package com.example.capstone.repository

import java.time.LocalDateTime

import com.example.common.Event
import com.example.common.Formats.dateTimeFormatter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

trait EventProvider extends Serializable{
  def readEventsRDD(): RDD[Event]
  def readEventsDF(): DataFrame

  def mapToEvent(date: String, price: String, category: String, productName: String, ip: String): Event = {
    Event(LocalDateTime.from(dateTimeFormatter.parse(date)), productName, BigDecimal(price), category, ip)
  }

}

object EventProvider {
  val eventsSchema = new StructType()
    .add(StructField("date", TimestampType))
    .add(StructField("price", DecimalType(10, 2)))
    .add(StructField("productName", StringType))
    .add(StructField("category", StringType))
    .add(StructField("clientIp", StringType))

  val columnNames = Seq("date", "price", "productName", "category", "clientIp")
}