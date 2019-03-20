package active

import com.typesafe.config.{Config, ConfigFactory}
import passive.StructuresStreaming.{args, props, queryNum}

object Main extends App {
  val config_file = ConfigFactory.load("application.conf")
  val props : Config  = ConfigFactory.load("application.properties")

  val TypeName = props.getConfig(args(0))
  val fixSlid = props.getConfig(args(1))

// object for consumer class
  //val ConsumerClass = new StructStreamConsumer;


  if(TypeName.getString("inputName")  == "weblogs") {

    println(" ======================================= Weblogs ===================================================")
    val filename_logs = config_file.getString("sorted_logs")
    val weblogs = new Functions(filename_logs); // object for weblogs

    // Kafka Topic for Weblogs
    val topic_weblogs = config_file.getString("topic1")
    val broker = config_file.getString("broker")


    // ========== Functions===============================================
    val firstLine_c = weblogs.first_line()
    println("first line weblogs " + firstLine_c)

    // ============================================================
    //gives only last line of a file
    var lastLine_c = weblogs.last_line()
    println("last line weblogs " + lastLine_c)

    // ===============================================================
    // returns the date of web_logs of a line
    var firstline_date_c = weblogs.get_timestamp_c(firstLine_c)
    println("date of first line of web logs " + firstline_date_c)

    var lastline_date_c = weblogs.get_timestamp_c(lastLine_c)
    //println("date of last line of weblogs " + lastline_date_c)

    // =====================================================================================
    // calculate the days between first and last timestamp
    val days_web_logs = weblogs.get_Days(firstline_date_c, lastline_date_c)
    println("# days between first and last line of weblogs " + days_web_logs)

    // =======================================================================================
    // Records to be send to Kafka per window
    // Fixed window
    val TotalWindows = config_file.getLong("Total_windows")
    val fixedWind_web_logs = weblogs.fixed_window(days_web_logs, TotalWindows, firstline_date_c)
    println("# days in a window " + fixedWind_web_logs )


    // ====================================================================================================
    // ====================================================================================================
    if(fixSlid.getString("inputType")  == "fixed") {
    println(" =========================== fixed window ==================================================")
      val weblogs_prod = new Kafka_Prod_weblogs(filename_logs);
      val weblog_fix = weblogs_prod.prod_weblogs(topic_weblogs ,fixedWind_web_logs , firstline_date_c , lastline_date_c , TotalWindows)

    }else if (fixSlid.getString("inputType")  == "sliding") {
      // ====================================================================================================
      // ====================================================================================================

      println(" ==================== sliding window =====================================")

    val slidding_logs = new Kafka_Prod_Log_Slid(filename_logs);
    val slid_logs = slidding_logs.prod_log_slid(topic_weblogs, fixedWind_web_logs, firstline_date_c, lastline_date_c)
    }else {
      println(" type fixed or sliding")
    }

  }else if (TypeName.getString("inputName")  == "websales") {

    // ====================================================================================================
    // ====================================================================================================
    println(" ======================================= Websales ===================================================")


    // FilePath read from congifFile
    val filename_sales = config_file.getString("sorted_sales")

    val websales = new Functions(filename_sales); // object for websales

    // Kafka Topic for websales
    val topic_websales = config_file.getString("topic2")
    val broker = config_file.getString("broker")



    // record first line
    val firstLine_s = websales.first_line()
    println("first line websales " + firstLine_s)

    //gives only last line of a file
    val lastLine_s = websales.last_line()
    println("last line websales " + lastLine_s)

    // ================================================================
    // return the date of web_sales of a line

    val firstline_date_s = websales.get_timestamp_s(firstLine_s)
    println("date of first line of websales " + firstline_date_s)

    val lastline_date_s = websales.get_timestamp_s(lastLine_s)
    println("date of last line of websales " + lastline_date_s)

    // =====================================================================================
    // calculate the days between first and last timestamp
    val days_web_sales = websales.get_Days(firstline_date_s, lastline_date_s)
    println("# days between first and last line of websales " + days_web_sales)

    // =======================================================================================
    // Records to be send to Kafka per window
    // Fixed window

    val TotalWindows = config_file.getLong("Total_windows")
    val fixedWind_web_sales = websales.fixed_window(days_web_sales, TotalWindows, firstline_date_s)
    println("#  days in a window " + fixedWind_web_sales)

    // ====================================================================================================
    // ====================================================================================================

    // =========================== Kafka Producer ==================================================
    if(fixSlid.getString("inputType")  == "fixed") {
      println(" =========================== fixed window ==================================================")

    val websales_prod = new Kafka_Prod_Sales(filename_sales);
    val websales_fix = websales_prod.prod_websales(topic_websales ,fixedWind_web_sales , firstline_date_s , lastline_date_s , TotalWindows)

    }else if (fixSlid.getString("inputType")  == "sliding") {

      println(" ==================== sliding window =====================================")

      val slidding_sales = new Kafka_Prod_Sales_Slid(filename_sales);
      val slid_sales = slidding_sales.prod_sales_slid(topic_websales , fixedWind_web_sales , firstline_date_s ,lastline_date_s)
    }else {
      println(" type fixed or sliding")
    }

  }else {
    println("type weblogs or websales")
  }
}

