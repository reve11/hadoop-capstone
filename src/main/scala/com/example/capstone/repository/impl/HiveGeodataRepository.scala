package com.example.capstone.repository.impl

import com.example.capstone.model.{GeoData, GeoIp}
import com.example.capstone.repository.GeodataRepository
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

class HiveGeodataRepository(context: SparkContext, ipsTableName: String, geodataTableName: String) extends GeodataRepository {

  private val sqlContext = new org.apache.spark.sql.SQLContext(context)
  import sqlContext.sql
  import sqlContext.implicits._

  private lazy val ipsDF: DataFrame = sql(s"select * from $ipsTableName")
  private lazy val ipsRDD: RDD[GeoIp] = ipsDF.map[GeoIp]((row: Row) => GeoIp(row.getString(0), row.getInt(1), row.getInt(2)))
  private lazy val geodataDF: DataFrame = sql(s"select * from $geodataTableName")
  private lazy val geodataRDD: RDD[GeoData] = geodataDF.map[GeoData]((row: Row) => GeoData(row.getInt(0), row.getString(1),
    row.getString(2), row.getString(3), row.getString(4), row.getString(5)))

  override def fetchGeoIpsRDD(): RDD[GeoIp] = ipsRDD

  override def fetchGeoIpsDF(): DataFrame = ipsDF

  override def fetchGeoDataRDD(): RDD[GeoData] = geodataRDD

  override def fetchGeoDataDF(): DataFrame = geodataDF
}
