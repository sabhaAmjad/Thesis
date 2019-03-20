package active

import java.io.{FileNotFoundException, IOException}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source

class Kafka_Prod_Sales (fileName : String)  {

  val config_file = ConfigFactory.load("application.conf")
  val TotalWindows = config_file.getLong("Total_windows")
  val sleepTime = config_file.getLong("sleep_time")

  val brokers = config_file.getString("broker")

  val props = new Properties()
  props.put("bootstrap.servers", brokers)
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)


  def prod_websales(topic: String, p: Long, first_line: java.time.LocalDate, lastline: java.time.LocalDate, TotalWindows: Long) {
    try {

      val lines = Source.fromFile(fileName).getLines()
      var record = lines.next

      val NEWLINE = 10
      var newlineCount = 0L

      var window_date_0 = first_line
      println("before 0 " + window_date_0)

      var window_date_1 = first_line.plusDays(p)
      println("before 1 " + window_date_1)

      val totalWindows = TotalWindows.toInt

      var cond = true

      while ((window_date_0.isBefore(lastline) == true) && (cond == true)) {
        //val instanz = new Functions("/home/sabha/BigBenchV2/Data/web-sales/sorted_websales.csv")
        //def fixwind_websales = instanz.get_timestamp_s(records)
        val instanz = new Functions(fileName)
        def fixwind_websales = instanz.get_timestamp_s(record)
      //var fixwind_websales = get_timestamp_s(records)

        val actual_date = println(fixwind_websales)
        //println("inside " + window_date)

        if ((fixwind_websales.isAfter(window_date_0) && fixwind_websales.isBefore(window_date_1)) == true) {
          println("between")
          producer.send(new ProducerRecord[String, String](topic, s"$actual_date" , s"$record" ))
          println(s"Sent: ${record.mkString} , ${actual_date}" )

          if (lines.hasNext == true ) {
          record = lines.next()
          def fixwind_websales = instanz.get_timestamp_s(record)
          println("new record date " +fixwind_websales)
          newlineCount += 1
          println("count " + newlineCount)}
          else {
            cond = false
          }
        }else if (fixwind_websales.isEqual(window_date_0) == true){
          println("equal to")
          producer.send(new ProducerRecord[String, String](topic, s"$actual_date" , s"$record" ))
          println(s"Sent: ${record.mkString} , ${actual_date}" )

          if (lines.hasNext == true ) {
            record = lines.next()
            def fixwind_websales = instanz.get_timestamp_s(record)
            println("new record date " +fixwind_websales)
            newlineCount += 1
            println("count " + newlineCount)}
          else {
            cond = false
          }
        }else if (fixwind_websales.isEqual(window_date_1) == true) {
          println("equal t1")
          producer.send(new ProducerRecord[String, String](topic, s"$actual_date", s"$record"))
          println(s"Sent: ${record.mkString} , ${actual_date}")

          if (lines.hasNext == true) {
            record = lines.next()

            def fixwind_websales = instanz.get_timestamp_s(record)

            println("new record date " + fixwind_websales)
            newlineCount += 1
            println("count " + newlineCount)
          }
          else {
            cond = false
          }

        }else {}
        if (fixwind_websales.isAfter(window_date_1) == true){
          println("waait")
          Thread.sleep(sleepTime)
          println(" next window")
          window_date_0 = window_date_0.plusDays(p)
          window_date_1 = window_date_1.plusDays(p)
          println("after 0 " + window_date_0)
          println("after 1 " + window_date_1)
        }else {
          println(" same window")
          window_date_0 = window_date_0
          window_date_1 = window_date_1
        }

      }
      producer.close()
    }catch{
      case e: FileNotFoundException => println("Couldn't find that file.")
      case e: IOException => println("Got an IOException!")
      case ex: Exception => ex.printStackTrace()
    }
  }
}