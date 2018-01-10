package com.example

import com.example.common.NetworkProvider
import org.scalatest.FlatSpec

class NetworkProviderTest extends FlatSpec {
  "Network " should "be found by IP address" in {
    for (_ <- 1 to 1000) {
      val ip = NetworkProvider.getRandomIp
      val maybeNetwork = NetworkProvider.getNetwork(ip.ip)
      println(ip)
      assert(maybeNetwork.isDefined)
      assert(maybeNetwork.get.toString.equals(ip.network))
    }
  }
}
