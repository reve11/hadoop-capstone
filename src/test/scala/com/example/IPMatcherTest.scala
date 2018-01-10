package com.example

import com.example.common.NetworkProvider
import org.scalatest.FlatSpec

class IPMatcherTest extends FlatSpec {
  "IP address" should "belong to its network" in {
    val matcher = new IPMatcher
    for (_ <- 0 until 100) {
      val ip = NetworkProvider.getRandomIp
      println(ip)
      assert(matcher.matches(ip.ip, ip.network).get())
    }
  }
}
