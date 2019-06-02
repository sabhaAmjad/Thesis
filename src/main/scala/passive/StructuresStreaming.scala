package passive

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._

object StructuresStreaming extends App {

  val config_file  = ConfigFactory.load("application.conf")

  val props : Config  = ConfigFactory.load("application.properties")
  val queryNum = props.getConfig(args(0))


  val local = config_file.getString("dev")
  val path_weblogs = config_file.getString("path_logs")
  val path_websales = config_file.getString("path_sales")

  val sparkSession = SparkSession.builder
    .appName("Thesis")
    .master(local)
    .config("spark.sql.streaming.metricsEnabled", true)
    //.config("spark.eventLog.enabled",true)
    .config("spark.logConf", true)
    .getOrCreate()



  val schema_web_sales = StructType(
    Array(StructField("ws_transaction_id", IntegerType),
      StructField("ws_user_id", IntegerType),
      StructField("ws_product_id", IntegerType),
      StructField("ws_quantity", FloatType),
      StructField("ws_timestamp", TimestampType)))

  val schema_web_logs = StructType(
    Array(StructField("wl_customer_id", LongType ,nullable = true),
      StructField("wl_id", LongType ,nullable = true),
      StructField("wl_item_id", LongType ,nullable = true),
      StructField("wl_key1", LongType ,nullable = true),
      StructField("wl_key10", LongType ,nullable = true),
      StructField("wl_key100", LongType ,nullable = true),
      StructField("wl_key11", LongType ,nullable = true),
      StructField("wl_key12", LongType ,nullable = true),
      StructField("wl_key13", LongType ,nullable = true),
      StructField("wl_key14", LongType ,nullable = true),
      StructField("wl_key15", LongType ,nullable = true),
      StructField("wl_key16", LongType ,nullable = true),
      StructField("wl_key17", LongType ,nullable = true),
      StructField("wl_key18", LongType ,nullable = true),
      StructField("wl_key19", LongType ,nullable = true),
      StructField("wl_key2", LongType ,nullable = true),
      StructField("wl_key20", LongType ,nullable = true),
      StructField("wl_key21", LongType ,nullable = true),
      StructField("wl_key22", LongType ,nullable = true),
      StructField("wl_key23", LongType ,nullable = true),
      StructField("wl_key24", LongType ,nullable = true),
      StructField("wl_key25", LongType ,nullable = true),
      StructField("wl_key26", LongType ,nullable = true),
      StructField("wl_key27", LongType ,nullable = true),
      StructField("wl_key28", LongType ,nullable = true),
      StructField("wl_key29", LongType ,nullable = true),
      StructField("wl_key3", LongType ,nullable = true),
      StructField("wl_key30", LongType ,nullable = true),
      StructField("wl_key31", LongType ,nullable = true),
      StructField("wl_key32", LongType ,nullable = true),
      StructField("wl_key33", LongType ,nullable = true),
      StructField("wl_key34", LongType ,nullable = true),
      StructField("wl_key35", LongType ,nullable = true),
      StructField("wl_key36", LongType ,nullable = true),
      StructField("wl_key37", LongType ,nullable = true),
      StructField("wl_key38", LongType ,nullable = true),
      StructField("wl_key39", LongType ,nullable = true),
      StructField("wl_key4", LongType ,nullable = true),
      StructField("wl_key40", LongType ,nullable = true),
      StructField("wl_key41", LongType ,nullable = true),
      StructField("wl_key42", LongType ,nullable = true),
      StructField("wl_key43", LongType ,nullable = true),
      StructField("wl_key44", LongType ,nullable = true),
      StructField("wl_key45", LongType ,nullable = true),
      StructField("wl_key46", LongType ,nullable = true),
      StructField("wl_key47", LongType ,nullable = true),
      StructField("wl_key48", LongType ,nullable = true),
      StructField("wl_key49", LongType ,nullable = true),
      StructField("wl_key5", LongType ,nullable = true),
      StructField("wl_key50", LongType ,nullable = true),
      StructField("wl_key51", LongType ,nullable = true),
      StructField("wl_key52", LongType ,nullable = true),
      StructField("wl_key53", LongType ,nullable = true),
      StructField("wl_key54", LongType ,nullable = true),
      StructField("wl_key55", LongType ,nullable = true),
      StructField("wl_key56", LongType ,nullable = true),
      StructField("wl_key57", LongType ,nullable = true),
      StructField("wl_key58", LongType ,nullable = true),
      StructField("wl_key59", LongType ,nullable = true),
      StructField("wl_key6", LongType ,nullable = true),
      StructField("wl_key60", LongType ,nullable = true),
      StructField("wl_key61", LongType ,nullable = true),
      StructField("wl_key62", LongType ,nullable = true),
      StructField("wl_key63", LongType ,nullable = true),
      StructField("wl_key64", LongType ,nullable = true),
      StructField("wl_key65", LongType ,nullable = true),
      StructField("wl_key66", LongType ,nullable = true),
      StructField("wl_key67", LongType ,nullable = true),
      StructField("wl_key68", LongType ,nullable = true),
      StructField("wl_key69", LongType ,nullable = true),
      StructField("wl_key7", LongType ,nullable = true),
      StructField("wl_key70", LongType ,nullable = true),
      StructField("wl_key71", LongType ,nullable = true),
      StructField("wl_key72", LongType ,nullable = true),
      StructField("wl_key73", LongType ,nullable = true),
      StructField("wl_key74", LongType ,nullable = true),
      StructField("wl_key75", LongType ,nullable = true),
      StructField("wl_key76", LongType ,nullable = true),
      StructField("wl_key77", LongType ,nullable = true),
      StructField("wl_key78", LongType ,nullable = true),
      StructField("wl_key79", LongType ,nullable = true),
      StructField("wl_key8", LongType ,nullable = true),
      StructField("wl_key80", LongType ,nullable = true),
      StructField("wl_key81", LongType ,nullable = true),
      StructField("wl_key82", LongType ,nullable = true),
      StructField("wl_key83", LongType ,nullable = true),
      StructField("wl_key84", LongType ,nullable = true),
      StructField("wl_key85", LongType ,nullable = true),
      StructField("wl_key86", LongType ,nullable = true),
      StructField("wl_key87", LongType ,nullable = true),
      StructField("wl_key88", LongType ,nullable = true),
      StructField("wl_key89", LongType ,nullable = true),
      StructField("wl_key9", LongType ,nullable = true),
      StructField("wl_key90", LongType ,nullable = true),
      StructField("wl_key91", LongType ,nullable = true),
      StructField("wl_key92", LongType ,nullable = true),
      StructField("wl_key93", LongType ,nullable = true),
      StructField("wl_key94", LongType ,nullable = true),
      StructField("wl_key95", LongType ,nullable = true),
      StructField("wl_key96", LongType ,nullable = true),
      StructField("wl_key97", LongType ,nullable = true),
      StructField("wl_key98", LongType ,nullable = true),
      StructField("wl_key99", LongType, nullable = true),
      StructField("wl_timestamp", StringType, nullable = true),
      StructField("wl_webpage_name", StringType, nullable = true)))


  //====================================================================
  //===================================================================

  // input query


  val web_logsDF = sparkSession.readStream
    .option("header", "false")
    .option("maxFilesPerTrigger",1)
    .schema(schema_web_logs)
    .json(path_weblogs)


  val web_salesDF = sparkSession.readStream
    .option("header", "false")
    .option("maxFilesPerTrigger",1)
    .schema(schema_web_sales)
    .format("csv")
    .load(path_websales)


  println(web_salesDF.isStreaming)
  println(web_logsDF.isStreaming)

  web_logsDF.printSchema()
  web_logsDF.createOrReplaceTempView("web_logs")

  web_salesDF.printSchema()
  web_salesDF.createOrReplaceTempView("web_sales")

  /**web_salesStaticDf.printSchema()
    web_salesStaticDf.createOrReplaceTempView("web_sales_static")*/


  /**=====================================================================
    * Set options for writestream
    */

  //print("Enter the number of querry: ")
  //  val UserInput : Int = scala.io.StdIn.readLine.toInt

  println("Queries start now")
  /** ============ Start Streamqueries ========== */


  if (queryNum.getString("inputMode")  == "s3") {

    println("query 16 is executing...")
    /**========= Query 16 ========== */
    var web_logs_16 = web_logsDF
      .groupBy("wl_webpage_name").count()
      .orderBy("count")
      .select("wl_webpage_name","count")
      .where("wl_webpage_name IS NOT NULL")

    var query16 = web_logs_16.writeStream
      .format("console")
      .queryName("+++16+++")

      /** .trigger(Trigger.ProcessingTime("150 seconds")) */
      .outputMode(OutputMode.Complete())
      .start()
    query16.awaitTermination()

  }else if (queryNum.getString("inputMode")  == "s4") {

    /**========== Query 22 =========== */
    println("query 22 is executing...")
    var web_logs_22 = web_logsDF
      .groupBy("wl_customer_id").count()
      .orderBy("count")
      .select("wl_customer_id","count")
      .where("wl_customer_id IS NOT NULL")

    var query22 = web_logs_22.writeStream
      .format("console")
      .queryName("22")
      .outputMode(OutputMode.Complete())
    //.start()
    /**query22.awaitTermination()*/

  }else if (queryNum.getString("inputMode")  == "s1") {

    println("query 5 is executing...")
    /** ========= Query 05 =========== */
    var web_logs_05 = web_logsDF
      .groupBy("wl_item_id").count()
      .orderBy("count")
      .select("wl_item_id","count").where("wl_item_id IS NOT NULL")

    var query05 = web_logs_05.writeStream
      .format("console")
      .queryName("+++05+++")
      /** .trigger(Trigger.ProcessingTime("150 seconds")) */
      .outputMode(OutputMode.Complete())
    /** .start() */
    /**query05.awaitTermination()*/
  }else if(queryNum.getString("inputMode")  == "s2") {
    println("query 6 is executing...")
    /** ========= Query 06 ========= */
    /** var browsedDF = web_logsDF
      .sparkSession.sql("SELECT wl_item_id AS br_id, COUNT(wl_item_id) AS br_count FROM web_logs WHERE wl_item_id IS NOT NULL GROUP BY wl_item_id")
      .createOrReplaceTempView("browsed")

    /**org.apache.log4j.filter.StringMatchFilter=*/


    var purchasedDF = web_salesStaticDf
      .sparkSession.sql("SELECT ws_product_id AS pu_id FROM web_sales_static WHERE ws_product_id IS NOT NULL GROUP BY ws_product_id")
      .createOrReplaceTempView("purchased")


    var query06 = web_logsDF
      .sparkSession.sql("SELECT br_id, COUNT(br_id) FROM browsed LEFT JOIN purchased ON browsed.br_id = purchased.pu_id WHERE purchased.pu_id IS NULL GROUP BY browsed.br_id")
      */


    /** var query06join = query06.writeStream
      * .format("console")
      * .outputMode(OutputMode.Complete()) */
    //.start()
    /**query06join.awaitTermination()*/
  }else if (queryNum.getString("inputMode")  == "s5"){

    println("query Milk is executing...")
    /** ========= Query Milk ========= */
    var web_sales_milk = web_salesDF
      .groupBy("ws_product_id").count()
      .orderBy("ws_product_id")
      .select("ws_product_id","count")
      .where("ws_product_ID IS NOT NULL")



    var querymilk = web_sales_milk.writeStream
      .format("console")
      .queryName("+++milk+++")

      /** .trigger(Trigger.ProcessingTime("80 seconds")) */
      .outputMode(OutputMode.Complete())

      .start()
    querymilk.awaitTermination()
  }else {
    print("queries are from s1 to s5")
  }

  sparkSession.stop()

}
