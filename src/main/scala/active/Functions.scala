package active

import java.time._
import java.time.format.DateTimeFormatter
import java.time.temporal._
import java.text.SimpleDateFormat
import java.util.Calendar

import com.typesafe.config.{Config, ConfigFactory}
import java.util._
import java.lang._
import scala.io.Source
import java.io._


class Functions(fileName :String) {

  // ==========================================================
  // gives first line of a file
  def first_line(): String = {
    val lines = Source.fromFile(fileName).getLines() //read lines
    val firstLine_c = (if (lines.hasNext) {
      lines.next
    } else None).toString
    //println(firstLine_c)
    return firstLine_c
  }

  // ==============================================================



  // ============================================================
  //gives only last line of a file
  def last_line(): String = {
    val lines = Source.fromFile(fileName).getLines() //read lines
    val lastLine_c = (lines.foldLeft(Option.empty[String]) { case (_, line) => Some(line) }.toString)
    //println(lastLine_c)
    return lastLine_c
  }

  // ===============================================================
  // returns the date of web_logs of a line
  def get_timestamp_c(line: String): java.time.LocalDate = {

    val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val index_l = line.indexOfSlice("wl_timestamp") // get the index of timestemp
    val substring_l = line.slice(index_l, index_l + 34)
    val index2_l = substring_l.split('"')
    var actual_date_l = LocalDate.parse(index2_l(2), dateTimeFormatter) // gives the timestemp of the line
    return actual_date_l
  }

  // ================================================================
  // return the date of web_sales of a line
  def get_timestamp_s(line: String): java.time.LocalDate = {

    val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    if (line.contains("Some(")) {
      val toRemove = "Some()".toSet
      val lines = line.filterNot(toRemove).split(',')
      var actual_date_s = LocalDate.parse(lines(4), dateTimeFormatter)
      return actual_date_s
    } else {
      val lines = line.split(',')
      var actual_date_s = LocalDate.parse(lines(4), dateTimeFormatter)
      return actual_date_s
    }
  }

  // =====================================================================================
  // calculate the days between first and last timestamp

  def get_Days(first_line: java.time.LocalDate, last_line: java.time.LocalDate): Long = {
    val days_diff = ChronoUnit.DAYS.between(first_line, last_line)

    return days_diff
  }

  // =======================================================================================
  // Records to be send to Kafka per window
  // Fixed window

  def fixed_window(takeDay: Long, totalWindows: Long, first_line: java.time.LocalDate): Long = {

      val p = takeDay / totalWindows
   return p

  }



  // =========================================================================================


  // ===========================end ofclass========================


}