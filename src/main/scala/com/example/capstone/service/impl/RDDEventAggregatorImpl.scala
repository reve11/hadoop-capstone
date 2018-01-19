package com.example.capstone.service.impl

import com.example.capstone.model._
import com.example.capstone.service.EventAggregator
import com.example.common.IpUtils.toBitString
import com.example.common.{Event, IpUtils, Network}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

class RDDEventAggregatorImpl(val eventsRDD: RDD[Event], val geoips: RDD[GeoIp], val geodata: RDD[GeoData]) extends EventAggregator {

  override def aggregateEvents(): AggregationResult = {
    val geoDataRDD = geodata.map(geodata => (geodata.countryId, geodata))
    val geoIpRDD = geoips.map(geoip => (geoip.id, geoip))
    val ipWithGeodata = geoDataRDD.join(geoIpRDD)
      .map(joined => CountryWithNetwork(new Network(joined._2._2.network), joined._2._1.countryId, joined._2._1.countryName))
      .collect()
    val byCategory = eventsRDD.groupBy(_.productCategory).persist

    (aggregateCategories(byCategory), aggregateProducts(eventsRDD), aggregateCountries(eventsRDD, ipWithGeodata))
  }

  private def aggregateCountries(eventsRdd: RDD[Event], ipWithGeodata: Array[CountryWithNetwork]): Iterable[TopCountry] = {
    def getGeoData(ip: String): Option[CountryWithNetwork] = {
      val addr = IpUtils.bitIpToLong(toBitString(ip))
      ipWithGeodata.find(c => c.network.minIpAddrNumeric() <= addr && c.network.maxIpAddrNumeric() >= addr)
    }

    eventsRdd.map(e => (getGeoData(e.clientIp), e.productPrice))
      .filter(_._1.nonEmpty)
      .map { case (country, price) => (country.get.countryName, price) }
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .map{case (countryName, spendings) => TopCountry(countryName, spendings)}
      .take(10)
  }

  private def aggregateProducts(events: RDD[Event]): Iterable[TopCategoryProduct] = {
    val byKey = events map (e => ((e.productCategory, e.productName), 1)) reduceByKey (_+_) map
      {case ((cat, name), count) => (cat, (name, count))}

    object CustOrdering extends Ordering[(String, Int)] {
      override def compare(x: (String, Int), y: (String, Int)): Int = Integer.compare(y._2, x._2)
    }
    def trimToSize(res: mutable.TreeSet[(String, Int)], size: Int) = {
      if (res.size > size) res.take(size) else res
    }

    byKey.aggregateByKey[mutable.TreeSet[(String, Int)]](mutable.TreeSet.empty[(String, Int)](CustOrdering))(
      (acc, p) => {
        trimToSize(acc += p, 5)
      },
      (acc1, acc2) => {
        trimToSize(acc1 ++= acc2, 5)
      })
      .flatMap{case (category, products) => products map {case (product, count) => TopCategoryProduct(category, product, count)}}
      .collect
  }

  private def aggregateCategories(byCategory: RDD[(String, Iterable[Event])]): Iterable[TopCategory] = {
    byCategory.sortBy(_._2.size, ascending = false)
      .take(10)
      .map(p => TopCategory(p._1, p._2.size))
  }

  case class CountryWithNetwork(network: Network, countryId: Int, countryName: String)
}
