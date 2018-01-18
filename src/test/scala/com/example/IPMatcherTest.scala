package com.example

import com.example.common.NetworkProvider
import org.scalatest.FlatSpec

class IPMatcherTest extends FlatSpec {
  "IP address" should "belong to its network" in {
    val matcher = new IPMatcher
    for (_ <- 0 until 10000) {
      val ip = NetworkProvider.getRandomIp
      val ip2 = NetworkProvider.getRandomIp
      println(String.format("%s - %s", ip.toString, ip2.toString))
      assert(matcher.matches(ip.ip, ip.network).get())
      assert(matcher.matches(ip2.ip, ip.network).get() == ip.network.equals(ip2.network))
    }
  }
}
