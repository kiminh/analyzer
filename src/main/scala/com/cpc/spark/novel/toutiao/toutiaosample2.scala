package com.cpc.spark.novel.toutiao

import java.io.StringReader
import au.com.bytecode.opencsv.CSVReader
import org.apache.spark.sql.{DataFrame, SparkSession}

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
      val reader = new CSVReader(new StringReader(line));
      reader.readNext()
    }
    println("alldata:"+result.count())


    val resultDF= result.map(x => {
      (x(0).toInt,x(1).toString())
    }).toDF("id", "title")
        .withColumn("id",when(id=0))

    resultDF.show(10)

//      .repartition(20).write.mode("overwrite").

  }
}
