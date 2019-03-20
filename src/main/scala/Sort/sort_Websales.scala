package Sort


import com.typesafe.config._
import org.apache.hadoop.fs._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

object sort_Websales {

  def main(args: Array[String]): Unit = {

    val config_file  = ConfigFactory.load("application.conf")
    val local = config_file.getString("dev")

    val path_websales = config_file.getString("path_hdfs_sales")

    val conf = new SparkConf().setMaster(local).setAppName("test")
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder
      .appName("Thesis")
      .master(local)
      .config("spark.logConf", true)
      .getOrCreate()

    val schema_web_sales = StructType(
      Array(StructField("ws_transaction_id", StringType),
        StructField("ws_user_id", StringType),
        StructField("ws_product_id", StringType),
        StructField("ws_quantity", StringType),
        StructField("ws_timestamp", StringType)))


    //===================================================================





    // DataFrame websales
    val websalesDF = spark.read.format("csv")
      .schema(schema_web_sales)
      .option("header", "false")
      .option("delimiter", "|")
      .load(path_websales)

    //websalesDF.show()

    websalesDF.createTempView("web_sales")

   val websales_df = spark.sql("select * from web_sales order by ws_timestamp asc")
    websales_df.show()

    val savefile = websales_df.coalesce(1).write.format("csv").option("header" , "false").mode(SaveMode.Append)
      .save("/home/sabha/BigBenchV2/DataSF1/web_sales/sorted_sales/")

    import org.apache.hadoop.fs.FileSystem
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val file = fs.globStatus(new Path("/home/sabha/BigBenchV2/DataSF1/web_sales/sorted_sales/part-00000-*"))(0).getPath.getName

    fs.rename(new Path("/home/sabha/BigBenchV2/DataSF1/web_sales/sorted_sales/" + file), new Path("/home/sabha/BigBenchV2/DataSF1/web_sales/sorted_sales.csv"))
    fs.delete(new Path("/home/sabha/BigBenchV2/DataSF1/web_sales/sorted_sales/_SUCCESS"), true)
  }

}
