package com.example.capstone.repository.impl

import com.example.capstone.repository.EventProvider
import com.example.common.Event
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

class HiveEventProvider(context: SparkContext, eventsTableName: String) extends EventProvider {

  private val sqlContext = new org.apache.spark.sql.SQLContext(context)
  import sqlContext.sql
  import sqlContext.implicits._

  private val eventsDF: DataFrame = sql(s"select * from $eventsTableName")
  private lazy val eventsRDD: RDD[Event] = eventsDF
    .map[Event]((row: Row) => mapToEvent(row.getString(0), row.getString(2), row.getString(1), row.getString(3), row.getString(4)))

  override def readEventsRDD(): RDD[Event] = eventsRDD

  override def readEventsDF(): DataFrame = eventsDF
}
