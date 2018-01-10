package com.example.capstone.service.impl

import com.example.capstone.model._
import com.example.capstone.service.EventAggregator
import com.example.common.IpUtils.toBitString
import com.example.common.{Event, IpUtils, Network}
import org.apache.spark.rdd.RDD

class RDDEventAggregatorImpl(val eventsRDD: RDD[Event], val geoips: RDD[GeoIp], val geodata: RDD[GeoData]) extends EventAggregator {

  override def aggregateEvents(): AggregationResult = {
    val geoDataRDD = geodata.map(geodata => (geodata.countryId, geodata))
    val geoIpRDD = geoips.map(geoip => (geoip.id, geoip))
    val ipWithGeodata = geoDataRDD.join(geoIpRDD)
      .map(joined => CountryWithNetwork(new Network(joined._2._2.network), joined._2._1.countryId, joined._2._1.countryName))
      .collect()
    val eventsRdd = eventsRDD
    val byCategory = eventsRdd.groupBy(_.productCategory).persist

    (aggregateCategories(byCategory), aggregateProducts(byCategory), aggregateCountries(eventsRdd, ipWithGeodata))
  }

  private def aggregateCountries(eventsRdd: RDD[Event], ipWithGeodata: Array[CountryWithNetwork]): Iterable[TopCountry] = {
    def getGeoData(ip: String): Option[CountryWithNetwork] = {
      val addr = IpUtils.bitIpToLong(toBitString(ip))
      ipWithGeodata.find(c => c.network.minIpAddrNumeric() <= addr && c.network.maxIpAddrNumeric() >= addr)
    }

    eventsRdd.groupBy(e => getGeoData(e.clientIp))
      .filter(_._1.nonEmpty)
      .map(p => (p._1.get, p._2.map(_.productPrice).sum))
      .groupBy(_._1.countryName)
      .map{ case (name, events) => (name, events.map(_._2).sum)}
      .sortBy(_._2, ascending = false)
      .take(10)
      .map(p => TopCountry(p._1, p._2))
  }

  private def aggregateProducts(byCategory: RDD[(String, Iterable[Event])]): Iterable[TopCategoryProduct] = {
    byCategory.flatMap{ case (category, events) =>
      events.groupBy(_.productName).toList.sortBy(_._2.size).map(p => (p._1, p._2.size)).reverse.take(5)
        .map(p => TopCategoryProduct(category, p._1, p._2))
    }.collect
  }

  private def aggregateCategories(byCategory: RDD[(String, Iterable[Event])]): Iterable[TopCategory] = {
    byCategory.sortBy(_._2.size, ascending = false)
      .take(10)
      .map(p => TopCategory(p._1, p._2.size))
  }

  case class CountryWithNetwork(network: Network, countryId: Int, countryName: String)
}
