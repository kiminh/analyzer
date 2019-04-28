package com.cpc.spark.novel.toutiao

import java.io.StringReader
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.functions._
import scala.sys.process._

object DspTitleClassify {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val date = args(0)
    val spark = SparkSession.builder()
      .appName(s"midu_sample")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val df = spark.read.format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "false") //是否自动推到内容的类型
      .option("delimiter","\001")  //分隔符，默认为 ,
      .load("hdfs://emr-cluster/user/cpc/wy/prediction.csv")
      df.show(10)

    val df2 = spark.read.format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "false") //是否自动推到内容的类型
      .option("delimiter","\001")  //分隔符，默认为 ,
      .load("hdfs://emr-cluster/user/cpc/wy/prediction_dsp.csv")
    df2.show(10)

    val adid=spark.sql("select * from dl_cpc.midu_toutiao_dsp_sample")

    val resultDF= df.union(df2).select($"_c0".alias("id"),$"_c1".alias("title"))
      .withColumn("category",convert(col("id")))
      .select("title","category")
      .join(adid,Seq("title"),"left")
      .withColumn("day",lit(s"$date"))

       resultDF.show(10)
      resultDF.repartition(1).write.mode("overwrite").insertInto("dl_cpc.midu_toutiao_adclass_predict")
  }

  def convert= udf{
    (x:String)=> {
      var y = ""
      x match {
        case "0.0" =>  y = "美容化妆"
        case "1.0" =>  y = "游戏类"
        case "2.0" =>  y = "社交网络"
        case "3.0" =>  y = "网上购物"
        case "4.0" =>  y = "二类电商"
        case "5.0" =>  y = "成人用品"
        case "6.0" =>  y = "生活服务"
        case "7.0" =>  y = "短视频"
        case "8.0" =>  y = "无"

      }
      y
    }
  }
}
