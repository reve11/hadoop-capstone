package com.example.capstone

import java.time.LocalDateTime

import com.example.capstone.model.{GeoData, GeoIp}
import com.example.capstone.repository.impl._
import com.example.capstone.repository.{EventProvider, GeodataRepository}
import com.example.capstone.service.EventAggregator
import com.example.capstone.service.impl.{DataframeEventAggregator, RDDEventAggregatorImpl}
import com.example.common.Event
import com.example.common.Formats.dateTimeFormatter
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}


object Main extends App {

  assert(args.length >= 3, () => "You should pass arguments in form aggregatorType dataSourceType sparkHost")

  println(s"Starting spark job on ${args(2)} with ${args(0)} aggregator, pulling events and geo data from ${args(1)}")

  val rdbmsString = "jdbc:mysql://localhost:3306/capstone"
  val countriesLocation = getClass.getResource("/GeoLite2-Country-Locations-en.csv").getPath
  val eventsLocation = getClass.getResource("/test_events").getPath
  val ipsLocation = getClass.getResource("/GeoLite2-Country-Blocks-IPv4.csv").getPath
  val eventsHiveTableName = "events"
  val ipsHiveTableName = "ips"
  val geodataHiveTableName = "countries"
  val dbName = "capstone"


  private val context = getSparkContext(args(2))
  private val sqlContext = new HiveContext(context)
  private val aggregator: Option[EventAggregator] = getAggregator(args(0))


  if (aggregator.isDefined) process(aggregator.get)

  def process(aggregator: EventAggregator): Unit = {
    val aggregated = aggregator.aggregateEvents()
    val resultConsumer = new StdoutAggregatedDataRepository
    resultConsumer.saveTopCountries(aggregated._3)
    resultConsumer.saveTopCategoryProducts(aggregated._2)
    resultConsumer.saveCategories(aggregated._1)
  }

  def getAggregator(name: String): Option[EventAggregator] = {
    if ("RDD".equalsIgnoreCase(name))
      Option(new RDDEventAggregatorImpl(getEvents(args(1))._1, getGeoIps(args(1))._1, getGeoData(args(1))._1))
    else if ("DF".equalsIgnoreCase(name))
      Option(new DataframeEventAggregator(sqlContext, getEvents(args(1))._2, getGeoIps(args(1))._2, getGeoData(args(1))._2))
    else Option.empty
  }

  def getEventsProvider(name: String): EventProvider = {
    if ("hive".equalsIgnoreCase(name)) {
      new HiveEventProvider(context, dbName + "." + eventsHiveTableName)
    } else {
      new FileEventProvider(eventsLocation, context)
    }
  }

  def getGeodataRepository(name: String): GeodataRepository = {
    if ("hive".equalsIgnoreCase(name)) {
      new HiveGeodataRepository(context, ipsHiveTableName, dbName + "." + geodataHiveTableName)
    } else {
      new FileGeodataRepository(countriesLocation, ipsLocation, context)
    }
  }

  def getSparkContext(name: String): SparkContext = {
    val conf: SparkConf = new SparkConf().setAppName("capstone")
    if ("local".equalsIgnoreCase(name)) {
      conf.setMaster("local[4]")
    }
    new SparkContext(conf)
  }

  def getEvents(source: String): (RDD[Event], DataFrame) = {
    if ("hive".equalsIgnoreCase(source)) {
      import sqlContext.sql
      val eventsDF: DataFrame = sql(s"select * from $dbName.$eventsHiveTableName")
      val eventsRDD: RDD[Event] = eventsDF
        .map[Event]((row: Row) => mapToEvent(row.getString(0), row.getString(2), row.getString(1), row.getString(3), row.getString(4)))
      (eventsRDD, eventsDF)
    } else {
      val eventsRDD = context.textFile(eventsLocation)
        .map(_.split(","))
        .map(line => mapToEvent(line(0), line(1), line(3), line(2), line(4)))
      val eventsDF = sqlContext.read.format("com.databricks.spark.csv")
        .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
        .load(eventsLocation)
        .toDF(EventProvider.columnNames: _*)
      (eventsRDD, eventsDF)
    }
  }

  private def getGeoIps(source: String): (RDD[GeoIp], Broadcast[DataFrame]) = {
    if ("hive".equalsIgnoreCase(source)) {
      import sqlContext.sql
      import sqlContext.implicits._
      val ipsDF: DataFrame = sql(s"select * from $dbName.$ipsHiveTableName")
      val ipsRDD: RDD[GeoIp] = ipsDF.map[GeoIp]((row: Row) => GeoIp(row.getString(0), row.getInt(1), row.getInt(2)))
      (ipsRDD, context.broadcast(ipsDF))
    } else {
      val ipsRDD = context.textFile(ipsLocation)
        .map(_.split(","))
        .filter(line => line.length >= 3)
        .filter(line => !line(1).isEmpty && !line(2).isEmpty)
        .map(line => GeoIp(line(0), line(1).toInt, line(2).toInt))
      val ipsDF: DataFrame = sqlContext.read.format("com.databricks.spark.csv")
        .schema(GeodataRepository.ipsSchema)
        .load(ipsLocation)
      (ipsRDD, context.broadcast(ipsDF))
    }
  }

  private def getGeoData(source: String): (RDD[GeoData], DataFrame) = {
    if ("hive".equalsIgnoreCase(source)) {
      import sqlContext.sql
      import sqlContext.implicits._
      val geodataDF: DataFrame = sql(s"select * from $dbName.$geodataHiveTableName")
      val geodataRDD: RDD[GeoData] = geodataDF.map[GeoData]((row: Row) => GeoData(row.getInt(0), row.getString(1),
        row.getString(2), row.getString(3), row.getString(4), row.getString(5)))
      (geodataRDD, geodataDF)
    } else {
      val countriesRDD = context.textFile(countriesLocation)
        .map(_.split(","))
        .filter(line => line.length == 6)
        .map(line => GeoData(line(0).toInt, line(1), line(2), line(3), line(4), line(5)))
      val countriesDF: DataFrame = sqlContext.read.format("com.databricks.spark.csv")
        .schema(GeodataRepository.countriesSchema)
        .load(countriesLocation)
      (countriesRDD, countriesDF)
    }
  }


  def mapToEvent(date: String, price: String, category: String, productName: String, ip: String): Event = {
    Event(LocalDateTime.from(dateTimeFormatter.parse(date)), productName, BigDecimal(price), category, ip)
  }
}
