package active

import java.io.{FileNotFoundException, IOException}

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import com.typesafe.config.{ Config, ConfigFactory }
import scala.io.Source

import scala.collection.mutable.Queue

class Kafka_Prod_Sales_Slid (fileName : String){

  val config_file = ConfigFactory.load("application.conf")
  val TotalWindows = config_file.getLong("Total_windows")
  val sleepTime = config_file.getLong("sleep_time")


  val brokers = config_file.getString("broker")

  val props = new  Properties()
  props.put("bootstrap.servers", brokers)
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  def prod_sales_slid ( topic : String  , p : Long, first_line: java.time.LocalDate , lastline :java.time.LocalDate )  {
    try {
      val lines = Source.fromFile(fileName).getLines()
      val NEWLINE = 10
      var newlineCount = 0L

      var window_date_0 = first_line
      var window_date_1 = first_line.plusDays(p/2)
      var window = println("first window [ " + window_date_0 +" , " + window_date_1 + " ]")

      var cond = true
      var record = lines.next()

      val instanz = new Functions(fileName)
      def fixwind_websales = instanz.get_timestamp_s(record)

      while (fixwind_websales.isBefore(window_date_1) == true && (cond == true)){
      println("first window")
      val actual_date = println(fixwind_websales)
      if ((fixwind_websales.isAfter(window_date_0) && fixwind_websales.isBefore(window_date_1)) == true) {
        println("between ")
        producer.send(new ProducerRecord[String, String](topic, s"$window", s"$record"))
        println(s"Sent: ${record.mkString} , ${window}")
        newlineCount += 1
        println("count odd " + newlineCount)
        if (lines.hasNext == true) {
          record = lines.next()
        }else {
          cond = false
        }
      } else if (fixwind_websales.isEqual(window_date_0) == true) {
        println("equal t0")
        producer.send(new ProducerRecord[String, String](topic, s"$window", s"$record"))
        println(s"Sent to odd: ${record.mkString} , ${window}")
        newlineCount += 1
        println("count odd " + newlineCount)
        if (lines.hasNext == true) {
          record = lines.next()
        }else {
          cond = false
        }
      } else if (fixwind_websales.isEqual(window_date_1) == true) {
        println("equal t1")
        producer.send(new ProducerRecord[String, String](topic, s"$window", s"$record"))
        println(s"Sent: ${record.mkString} , ${window}")
        newlineCount += 1
        println("count " + newlineCount)
        if (lines.hasNext == true) {
          record = lines.next()
        }else {
          cond = false
        }
      }}

      var newWindowCount = 0L
      newWindowCount += 1
      println("window count" + newWindowCount)
      var window_date_0_5 = first_line.plusDays(p/2)
      //println(" t1 " + window_date_0_5)
      var window_date_1_5 = window_date_0_5.plusDays(p/2)
      println("next window ( " + window_date_0_5 +" , " + window_date_1_5 +" ]")

      //empty queue
      var queue_record: Queue[String] = Queue.empty[String]
      // How to initialize an empty Queue
      queue_record = Queue()
      println(s" Queue = $queue_record")


      while ( (window_date_0_5.isBefore(lastline) == true) && (cond == true)  && (newWindowCount < 2*TotalWindows )) {

        println("next record")
        val actual_date = println(fixwind_websales)
        if ((fixwind_websales.isAfter(window_date_0_5) && fixwind_websales.isBefore(window_date_1_5)) == true) {
          println("between ")
          producer.send(new ProducerRecord[String, String](topic, s"$window", s"$record"))
          println(s"Sent to odd: ${record.mkString} , ${window}")


          queue_record.enqueue(record)
          println(queue_record)

          if (lines.hasNext == true) {
            record = lines.next()
          } else {
            cond = false
          }
        }else if (fixwind_websales.isEqual(window_date_0_5) == true) {
          println("equal t0")
          producer.send(new ProducerRecord[String, String](topic, s"$window", s"$record"))
          println(s"Sent to odd: ${record.mkString} , ${window}")

          queue_record.enqueue(record)
          println(queue_record)

          if (lines.hasNext == true) {
            record = lines.next()
          } else {
            cond = false
          }

        }else if (fixwind_websales.isEqual(window_date_1_5) == true) {
          println("equal t1")
          producer.send(new ProducerRecord[String, String](topic, s"$window", s"$record"))
          println(s"Sent to odd: ${record.mkString} , ${window}")


          queue_record.enqueue(record)
          println(queue_record)

          if (lines.hasNext == true) {
            record = lines.next()
          } else {
            cond = false
          }
        }

        if (fixwind_websales.isAfter(window_date_1_5) == true) {
          println(" next window")
          window_date_0_5 = window_date_0_5.plusDays(p/2)
          window_date_1_5 = window_date_1_5.plusDays(p/2)
          println(" [ " + window_date_0_5 + " , " + window_date_1_5 + " ]")
          newWindowCount += 1
          println(s"window count" + newWindowCount)

          if(queue_record != 0){
            println("wait")
            Thread.sleep(sleepTime)
            var queue_leer = queue_record.dequeueAll(_.length >= 1)
            println(queue_record)
            producer.send(new ProducerRecord[String, String](topic, s"$window", s"$queue_leer"))
            println(s"Sent again to next winow: ${queue_leer.mkString} , ${window}")
          }
          //println("after 1 odd " + window_date_1)
        } else {
          println(" same window")
          window_date_0_5 = window_date_0_5
          window_date_1_5 = window_date_1_5
        }

      }

      println("last window")
      var window_date_2nd_last = window_date_0_5
      var window_date_last = window_date_0_5.plusDays(p/2)
      println("last window [ " + window_date_2nd_last +" , " + window_date_last + " ]")
      println(fixwind_websales)
      while (fixwind_websales.isAfter(window_date_2nd_last) == true && (cond == true)){
      if ((fixwind_websales.isAfter(window_date_2nd_last) && fixwind_websales.isBefore(window_date_last)) == true) {
        println("between ")
        producer.send(new ProducerRecord[String, String](topic, s"$window", s"$record"))
        println(s"Sent: ${record.mkString} , ${window}")
        newlineCount += 1
        println("count odd " + newlineCount)
        if (lines.hasNext == true) {
          record = lines.next()
        }else{cond = false}
      } else if (fixwind_websales.isEqual(window_date_2nd_last) == true) {
        println("equal t0")
        producer.send(new ProducerRecord[String, String](topic, s"$window", s"$record"))
        println(s"Sent to odd: ${record.mkString} , ${window}")

        newlineCount += 1
        println("count " + newlineCount)

        if (lines.hasNext == true) {
          record = lines.next()
        } else {
          cond = false
        }

      }else if (fixwind_websales.isEqual(window_date_last) == true) {
        println("equal t1")
        producer.send(new ProducerRecord[String, String](topic, s"$window", s"$record"))
        println(s"Sent: ${record.mkString} , ${window}")
        newlineCount += 1
        println("count " + newlineCount)
        if (lines.hasNext == true) {
          record = lines.next()
        }else{cond = false}
      }
      }

      producer.close()
    }catch {
      case e: FileNotFoundException => println("Couldn't find that file.")
      case e: IOException => println("Got an IOException!")
      case ex: Exception => ex.printStackTrace()
    }
  }

}
