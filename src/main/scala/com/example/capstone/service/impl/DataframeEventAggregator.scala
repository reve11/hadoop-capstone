package com.example.capstone.service.impl

import com.example.capstone.model._
import com.example.capstone.service.EventAggregator
import com.example.common.{IpUtils, Network}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, rank, udf}

class DataframeEventAggregator(val sqlContext: SQLContext, val eventsDF: DataFrame, val geoips: DataFrame,
                               val geodata: DataFrame)
  extends EventAggregator{

  import sqlContext.implicits._

  override def aggregateEvents(): AggregationResult = {

    (aggregateCategories(eventsDF),
      aggregateProducts(eventsDF),
      aggregateCountries(eventsDF))
  }

  private def aggregateCountries(eventsDF: DataFrame): Iterable[TopCountry] = {

    val geodataDF = geodata.as("geodata")
    val geoipDF = geoips.as("ips")
    val ipInNetworkUDF = udf { (ip: String, network: String) => {
      val ipNumeric = IpUtils.bitIpToLong(IpUtils.toBitString(ip))
      val parsedNetwork = new Network(network)
      parsedNetwork.maxIpAddrNumeric() >= ipNumeric && parsedNetwork.minIpAddrNumeric() <= ipNumeric
    }}

    eventsDF.join(geoipDF, ipInNetworkUDF(col("events.clientIp"), col("ips.network")))
      .groupBy("ips.countryId")
      .agg(functions.sum("price").as("totalPrice"))
      .join(geodataDF, col("countryId") === col("geodata.id"))
      .select("geodata.countryName", "totalPrice")
      .orderBy($"totalPrice".desc)
      .limit(10)
      .map(row => TopCountry(row.getString(0), row.getDecimal(1)))
      .collect()
  }

  private def aggregateProducts(eventsDF: DataFrame): Iterable[TopCategoryProduct] = {

    val overCategory = Window.partitionBy($"category").orderBy($"count".desc, $"productName".asc)
    eventsDF.groupBy("productName", "category").count()
      .withColumn("rank", rank.over(overCategory)).where($"rank" <= 5).rdd
      .map(r => TopCategoryProduct(r.getString(1), r.getString(0), r.getLong(2).toInt))
      .collect
  }

  private def aggregateCategories(eventsDF: DataFrame): Iterable[TopCategory] = {

    eventsDF.groupBy("category").count().sort($"count".desc).limit(10).rdd
      .map(r => TopCategory(r.getString(0), r.getLong(1).toInt))
      .collect
  }
}
