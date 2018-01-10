package com.example.common

import java.text.DecimalFormat
import java.time.format.DateTimeFormatter

object Formats {
  val dateTimeFormatter: DateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME
  val decimalFormat = new DecimalFormat("##.00")
}
