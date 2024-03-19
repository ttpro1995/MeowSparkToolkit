package me.thaithien.sparktoolkit.common.support

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, LocalDateTime}
import java.util.TimeZone
import scala.collection.immutable

case class DateTime(pattern: String) {

  object Formatter {
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern(pattern)
  }

  import Formatter.formatter

  val string2date: String => LocalDate = (date_string: String) => {
    LocalDate.parse(date_string, formatter)
  }

  val date2string: LocalDate => String = (date: LocalDate) => {
    date.format(formatter)
  }

  val timestamp2date: Long => LocalDate = (timestamp: Long) => {
    LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp * 1000), TimeZone.getDefault.toZoneId).toLocalDate
  }

  val timestamp2string: Long => String = (timestamp: Long) => {
    LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp * 1000), TimeZone.getDefault.toZoneId).toLocalDate
      .format(formatter)
  }

  def get_dates_in_duration(date_start: Any, duration: Int, skip: Double = 1.0): immutable.Seq[String] = {
    date_start match {
      case date_string: String => Range(0, math.ceil(duration / skip).toInt).map(d =>
        date2string(string2date(date_string).plusDays(-d * skip.toInt)))
      case date: LocalDate => Range(0, math.ceil(duration / skip).toInt).map(d =>
        date2string(date.plusDays(-d * skip.toInt)))
      case _ => List()
    }
  }
}
