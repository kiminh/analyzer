package com.cpc.spark.OcpcProtoType.pcocForEldsAndApiCallBack

import org.apache.spark.sql.{DataFrame, SparkSession}

object WriteCsv {
  def main(args: Array[String]): Unit = {

  }

  def writeCsv(dataDF: DataFrame, path: String, spark: SparkSession): Unit ={
    val title = spark.sparkContext.parallelize(Seq(Seq("unitid", "pre_cvr",
    "post_cvr", "pcoc", "date", "type").mkString(","))).map(x => (x, 1))
    val newDataDF = dataDF.filter("pcoc is not null")
    val data = title.union(newDataDF.rdd.map(x => Seq(x.getAs[Int]("unitid").toString,
      x.getAs[Double]("pre_cvr").toString,
      x.getAs[Double]("post_cvr").toString,
      x.getAs[Double]("pcoc").toString,
      x.getAs[String]("date").toString,
      x.getAs[String]("type").toString).mkString(","))
      .map(x => (x, 2)))
      .repartition(1)
      .sortBy(_._2)
      .map(x => x._1)
    val list = data.collect()
    for(item <- list) println(item)
    data.saveAsTextFile(path)
  }
}
