package Sort

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}


object sort_weblogs {

  def main(args: Array[String]): Unit = {

    val config_file  = ConfigFactory.load("application.conf")
    val local = config_file.getString("dev")

    val path_weblogs = config_file.getString("path_hdfs_logs")
    //val path_websales = config_file.getString("path_hdfs_sales")

    val conf = new SparkConf().setMaster(local).setAppName("test")
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder
      .appName("Thesis")
      .master(local)
      .config("spark.logConf", true)
      .getOrCreate()

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

    //val pathweb_logs = ("/home/sabha/BigBenchV2/Data/web-logs/out100.json")

    val web_logsDF = spark.read.format("json")
      .option("header", "false")
      .option("mode" , "FAILFAST")
      .schema(schema_web_logs)
      .load(path_weblogs)

    web_logsDF.show()

    web_logsDF.createTempView("web_logs")

    val weblogs_df = spark.sql("select * from web_logs order by wl_timestamp asc")
    weblogs_df.show()

    val savefile = weblogs_df.coalesce(1).write.format("json").option("header" , "false").mode(SaveMode.Append)
      .save("/home/sabha/BigBenchV2/DataSF1/web_logs/sorted_logs/")

    import org.apache.hadoop.fs.FileSystem
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val file = fs.globStatus(new Path("/home/sabha/BigBenchV2/DataSF1/web_logs/sorted_logs/part-00000-*.json"))(0).getPath.getName

    fs.rename(new Path("/home/sabha/BigBenchV2/DataSF1/web_logs/sorted_logs/" + file), new Path("/home/sabha/BigBenchV2/DataSF1/web_logs/sorted_logs.json"))
    fs.delete(new Path("/home/sabha/BigBenchV2/DataSF1/web_logs/sorted_logs/_SUCCESS"), true)

  }

}
