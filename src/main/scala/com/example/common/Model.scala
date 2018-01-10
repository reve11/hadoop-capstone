package com.example.common

import java.time.LocalDateTime


class Network(val addr: String, val mask: Int) extends Serializable {

  def this(cid: String) {
    this(cid.split("/")(0), cid.split("/")(1).toInt)
  }

  private val parsedAddr = IpUtils.toBitString(addr)

  private lazy val maxAddrNumeric = IpUtils.bitIpToLong(parsedAddr.substring(0, mask) + (mask until 32).map(_ => "1").foldLeft("")(_ + _))
  private lazy val minAddrNumeric = IpUtils.bitIpToLong(parsedAddr.substring(0, mask) + (mask until 32).map(_ => "0").foldLeft("")(_ + _))


  def maxIpAddrNumeric(): Long = maxAddrNumeric

  def minIpAddrNumeric(): Long = minAddrNumeric

  override def toString: String = addr + "/" + mask
}

case class IPAddr(ip: String, network: String)
case class Event(purchaseDate: LocalDateTime, productName: String, productPrice: BigDecimal, productCategory: String,
                 clientIp: String)