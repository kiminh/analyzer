package com.cpc.spark.novel.toutiao

import java.io.StringReader
import au.com.bytecode.opencsv.CSVReader
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.functions._
import scala.sys.process._

object toutiaosample2 {
  def main(args: Array[String]): Unit = {
    val date = args(0)
    val spark = SparkSession.builder()
      .appName(s"midu_sample")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

//    val input = spark.sparkContext.textFile("/user/cpc/wy/prediction.csv")
//    val result = input.map{ line =>
//      val reader = new CSVReader(new StringReader(line))
//      reader.readNext()
//    }
//    println("alldata:"+result.count())
    val df = spark.read.format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "false") //是否自动推到内容的类型
      .option("delimiter","\001")  //分隔符，默认为 ,
      .load("/user/cpc/wy/prediction.csv")
      df.show(50)

//    val resultDF= titleDF
//      .withColumn("category",convert(col("id")))
//      .select("title","category")
//
//       resultDF.show(10)
//      resultDF.repartition(1).write.mode("overwrite").saveAsTable("dl_cpc.midu_toutiao_adclass_predict")
  }

  def convert= udf{
    (x:String)=> {
      var y = ""
      x match {
        case "0" =>  y = "美容化妆"
        case "1" =>  y = "游戏类"
        case "2" =>  y = "社交网络"
        case "3" =>  y = "网上购物"
        case "4" =>  y = "二类电商"
        case "5" =>  y = "成人用品"
        case "6" =>  y = "生活服务"
        case "7" =>  y = "无"

      }
      y
    }
  }
}
