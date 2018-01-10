package com.example.capstone.repository

import com.example.capstone.model.{GeoData, GeoIp}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

trait GeodataRepository extends Serializable {
  def fetchGeoIpsRDD(): RDD[GeoIp]
  def fetchGeoIpsDF(): DataFrame
  def fetchGeoDataRDD(): RDD[GeoData]
  def fetchGeoDataDF(): DataFrame
}

object GeodataRepository {
  val countriesSchema = new StructType()
    .add(StructField("id", LongType))
    .add(StructField("localeCode", StringType))
    .add(StructField("continentCode", StringType))
    .add(StructField("continentName", StringType))
    .add(StructField("countryCode", StringType))
    .add(StructField("countryName", StringType))

  val ipsSchema = new StructType()
    .add(StructField("network", StringType))
    .add(StructField("id", LongType))
    .add(StructField("countryId", LongType))
    .add(StructField("representedCountryId", LongType))
    .add(StructField("isProxy", IntegerType))
    .add(StructField("isSatellite", IntegerType))
}