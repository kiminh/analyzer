package com.cpc.spark.novel.toutiao

import java.io.StringReader
import au.com.bytecode.opencsv.CSVReader
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object toutiaosample2 {
  def main(args: Array[String]): Unit = {
    val date = args(0)
    val spark = SparkSession.builder()
      .appName(s"midu_sample")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val input = spark.sparkContext.textFile("/user/cpc/wy/prediction.csv")
    val result = input.map{ line =>
      val reader = new CSVReader(new StringReader(line))
      reader.readNext()
    }
    println("alldata:"+result.count())

    val resultDF= result.map(x => {
      (x(0).toString,x(1).toString)
    }).toDF("id", "title")
//      .withColumn("id",convert(col("id")))

    resultDF.show(10)
    resultDF.printSchema()

//      .repartition(20).write.mode("overwrite").
  }

  def convert= udf{
    (x:String)=> {
      var y = ""
      x match {
        case "0" =>  y = "美容化妆"
        case "1" =>  y = "游戏类"
        case "2" =>  y = "社交网络"
        case "3" =>  y = "游戏类"
        case "4" =>  y = "网上购物"
        case "5" =>  y = "无"

      }
      y
    }
  }
}
