package com.example.capstone.repository.impl

import com.example.capstone.repository.EventProvider
import com.example.common.Event
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

class FileEventProvider(filePath: String, context: SparkContext) extends EventProvider {

  private val sqlContext = new org.apache.spark.sql.SQLContext(context)

  private lazy val eventsRDD = context.textFile(filePath)
    .map(_.split(","))
    .map(line => mapToEvent(line(0), line(1), line(3), line(2), line(4)))

  private lazy val eventsDF = sqlContext.read.option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss").schema(EventProvider.eventsSchema).text(filePath)

  override def readEventsRDD(): RDD[Event] = eventsRDD

  override def readEventsDF(): DataFrame = eventsDF.as("events")
}
