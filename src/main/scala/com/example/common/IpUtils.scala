package com.example.common

import java.lang.{Long => javaLong}



object IpUtils {

  private def numericToBinaryWithLength(ip: Long, length: Int = 8): String = {
    val binary = ip.toBinaryString
    if (binary.length < length) ((0 until (length - binary.length)) map (_ => "0") reduce(_+_)) + binary
    else binary
  }

  def getNetwork(ip: String): Option[Network] = {
    NetworkProvider.getNetworks.find(isIpInNetwork(toBitString(ip), _))
  }

  private def isIpInNetwork(ipBitString: String, network: Network): Boolean = {
    val numericIp = bitIpToLong(ipBitString)
    val maxIpNumeric = network.maxIpAddrNumeric()
    val minIpNumeric = network.minIpAddrNumeric()
    numericIp >= minIpNumeric && numericIp <= maxIpNumeric
  }

  def bitIpToLong(ipString: String): javaLong = {
    javaLong.parseUnsignedLong(ipString, 2)
  }

  def toBitString(ipNumbericString: String): String =
    (ipNumbericString.split("\\.") map (_.toInt) map (numericToBinaryWithLength(_))) reduce(_+_)

  def toBinaryStringIp(ip: javaLong): String = {
    val binaryIp = numericToBinaryWithLength(ip, 32)
    ((0 to 32 by 8) zip (0 to 32 by 8).tail)
      .map{case (start, finish) => binaryIp.substring(start, finish)}
      .map(bitIpToLong)
      .map(_.toString)
      .reduceLeft(_ + "." +_)
  }
}
