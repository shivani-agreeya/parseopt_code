package com.verizon.oneparser.utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

case object DateUtils {

  val DATE_FORMAT = "MMddyyyy"
  val TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX")

  def millisToString = (millis: Long) => getDate(millis, DATE_FORMAT)

  private def getDate(milliSeconds: Long, dateFormat: String): String = {
    val formatter = new SimpleDateFormat(dateFormat)
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(milliSeconds)
    formatter.format(calendar.getTime)
  }

  def millisToTimestamp = (millis: Long) => TIMESTAMP_FORMAT.format(new Date(millis))
}
