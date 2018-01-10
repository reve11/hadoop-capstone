package com.example.common

import java.io.{File, FileReader}
import java.util

import com.example.common.IpUtils.toBitString
import com.opencsv.CSVReaderBuilder

import scala.collection.JavaConverters._
import scala.util.Random

object NetworkProvider {
  private val ipFile = new File(this.getClass.getClassLoader.getResource("GeoLite2-Country-Blocks-IPv4.csv").toURI)
  private val reader = new CSVReaderBuilder(new FileReader(ipFile)).build()
  private val networks: List[Network] = reader.readAll().asScala.map(line => new Network(line(0).split("/")(0), line(0).split("/")(1).toInt)).toList
  reader.close()
  private val networkIntervals: util.Map[NetworkInterval, Network] = new util.TreeMap[NetworkInterval, Network]
  networks.foreach(n => networkIntervals.put(new NetworkInterval(n), n))
  private val length: Int = networks.size
  private val random = new Random

  def getNetworks: List[Network] = networks

  def getNetwork(binaryIp: String): Option[Network] = {
    val addr = new IpAddr(IpUtils.bitIpToLong(toBitString(binaryIp)))
    Option(networkIntervals.get(addr))
  }

  def getRandomIp: IPAddr = {
    val network = networks(random.nextInt(length))
    val maxAddr = network.maxIpAddrNumeric()
    val minAddr = network.minIpAddrNumeric()
    val diff = (maxAddr - minAddr).toInt

    val randomAddrPart = if (diff == 0) 0 else random.nextInt(diff)

    IPAddr(IpUtils.toBinaryStringIp(minAddr + randomAddrPart), network.toString)
  }

  class NetworkInterval(min: Long, max: Long) extends Comparable[NetworkInterval]{
    val high: Long = max
    val low: Long = min

    def this(network: Network) {
      this(network.minIpAddrNumeric(), network.maxIpAddrNumeric())
    }

    override def compareTo(o: NetworkInterval): Int = o match {
      case ip: IpAddr => if (ip.addr <= high && ip.addr >= low) 0 else java.lang.Long.compare(low, ip.addr)
      case interval: NetworkInterval => java.lang.Long.compare(low, interval.low)
    }
  }

  class IpAddr(val addr: Long) extends NetworkInterval(addr, addr) {
    override def compareTo(o: NetworkInterval): Int = o match {
      case ip: IpAddr => java.lang.Long.compare(addr, ip.addr)
      case interval: NetworkInterval => if (addr >= interval.low && addr <= interval.high) 0 else java.lang.Long.compare(addr, interval.low)
    }
  }

}
