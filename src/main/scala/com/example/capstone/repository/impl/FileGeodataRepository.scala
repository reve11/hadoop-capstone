package com.example.capstone.repository.impl

import com.example.capstone.model.{GeoData, GeoIp}
import com.example.capstone.repository.GeodataRepository
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

class FileGeodataRepository(countriesFilePath: String, ipsFilePath: String, context: SparkContext) extends GeodataRepository {

  private val sqlContext = new org.apache.spark.sql.SQLContext(context)

  private lazy val countriesRDD = context.textFile(countriesFilePath)
    .map(_.split(","))
    .filter(line => line.length == 6)
    .map(line => GeoData(line(0).toInt, line(1), line(2), line(3), line(4), line(5)))

  private lazy val ipsRDD = context.textFile(ipsFilePath)
    .map(_.split(","))
    .filter(line => line.length >= 3)
    .filter(line => !line(1).isEmpty && !line(2).isEmpty)
    .map(line => GeoIp(line(0), line(1).toInt, line(2).toInt))

  private lazy val countriesDF: DataFrame = sqlContext.read.option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
    .schema(GeodataRepository.countriesSchema).text(countriesFilePath)

  private lazy val ipsDF: DataFrame = sqlContext.read.option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
    .schema(GeodataRepository.ipsSchema).text(ipsFilePath)

  override def fetchGeoIpsRDD(): RDD[GeoIp] = ipsRDD

  override def fetchGeoDataRDD(): RDD[GeoData] = countriesRDD

  override def fetchGeoIpsDF() = ipsDF

  override def fetchGeoDataDF() = countriesDF
}
