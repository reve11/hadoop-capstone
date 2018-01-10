package com.example.generator

import java.io._
import java.net.Socket
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

import com.example.common.Formats.{dateTimeFormatter, decimalFormat}
import com.example.common.{Event, NetworkProvider}
import com.example.generator.Constants.categories
import com.opencsv.CSVWriter

import scala.util.Random

object EventGenerator {
  private val random = new Random
  private val socket: Socket = new Socket("localhost", 9000)
  private val writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream))
  private val beanToCsv = new CSVWriter(writer, CSVWriter.DEFAULT_SEPARATOR, CSVWriter.NO_QUOTE_CHARACTER,
    CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END)

  def main(args: Array[String]): Unit = {
    for (_ <- 0 to 20) {
      sendMessage(genMessage)
    }
    beanToCsv.close()
    writer.close()
    socket.close()
  }

  def sendMessage(event: Event): Unit = {
    beanToCsv.writeNext(Array(dateTimeFormatter.format(event.purchaseDate),
      decimalFormat.format(event.productPrice.doubleValue()), event.productName, event.productCategory, event.clientIp))
  }

  def genMessage: Event = {
    val category = genCategory
    Event(genDate, genName(category), BigDecimal.valueOf(genPrice), category, NetworkProvider.getRandomIp.ip)
  }

  def genCategory: String = categories(random.nextInt(categories.size))
  def genName(category: String): String = category + random.nextInt(10)

  def genPrice: Double = genGaussianPositive * random.nextInt(1000)

  def genGaussianPositive: Double = {
    val res = random.nextGaussian
    if (res < 0) -res else res
  }

  def genUniform: Double = random.nextDouble

  def genDate: LocalDateTime = LocalDateTime.now()
    .minusDays(random.nextInt(7))
    .truncatedTo(ChronoUnit.DAYS)
    .plusHours((genGaussianPositive * 23).toInt)
    .plusMinutes((genGaussianPositive * 59).toInt)
    .plusSeconds((genGaussianPositive * 59).toInt)

}
