package com.cpc.spark.ml.ctrmodel.hourly


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.mutable.Map
import com.cpc.spark.qukan.utils.Udfs._
import org.apache.spark.sql.functions.{col, concat_ws, lit, udf, when}

object DNNSample {

  var mediaIdMap = Map[Int, Int]()
  var planIdMap = Map[Int, Int]()
  var unitIdMap = Map[Int, Int]()
  var ideaIdMap = Map[Int, Int]()
  var adslotIdMap = Map[Int, Int]()
  var cityMap = Map[Int, Int]()
  var adclassMap = Map[Int, Int]()
  var brandMap = Map[String, Int]()

  var uidMap = Map[String, Int]()

  var currentMaxIdx = 1


  def main(args: Array[String]): Unit = {

    val date = args(0)
    val hour = args(1)
    val dateList = List(date)

    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .appName("dnn sample")
      .enableHiveSupport()
      .getOrCreate()

    genIntMap(spark, mediaIdMap, "mediaid", dateList)
    genIntMap(spark, planIdMap, "planid", dateList)
    genIntMap(spark, unitIdMap, "unitid", dateList)
    genIntMap(spark, ideaIdMap, "ideaid", dateList)
    genIntMap(spark, adslotIdMap, "slotid", dateList)
    genIntMap(spark, cityMap, "cityid", dateList)
    genIntMap(spark, adclassMap, "adclass", dateList)

    // genStrMap(spark, brandMap, "brand", dateList)

    println(s"max index = $currentMaxIdx")


    val sql =
      """
        | select label,
        |   media_appsid as mediaid,
        |   planid, unitid, ideaid, adslotid,
        |   city, adclass
        | from dl_cpc.ml_ctr_feature_v1
        | where `date` = '$date' and hour ='$hour' and
        | media_appsid in (80000001, 80000002)
        | and adslot_type in (1)
      """.stripMargin

    println(sql)

    val sample0 = spark.sql(sql).limit(10000)
    getStrMapByDataset(spark, uidMap, "uid", sample0)

    println(s"max index = $currentMaxIdx")

    val sample = sample0
      .withColumn("mediaid-new", udfIntToIndex(mediaIdMap.toMap)(col("mediaid")))
      .withColumn("planid-new", udfIntToIndex(planIdMap.toMap)(col("planid")))
      .withColumn("unitid-new", udfIntToIndex(unitIdMap.toMap)(col("unitid")))
      .withColumn("ideaid-new", udfIntToIndex(ideaIdMap.toMap)(col("ideaid")))
      .withColumn("adslotid-new", udfIntToIndex(adslotIdMap.toMap)(col("adslotid")))
      .withColumn("city-new", udfIntToIndex(cityMap.toMap)(col("city")))
      .withColumn("adclass-new", udfIntToIndex(adclassMap.toMap)(col("adclass")))
      .withColumn("uid-new", udfStrToIndex(uidMap.toMap)(col("uid")))
      .withColumn("sample", concat_ws("\t",
        col("label"),
        col("mediaid-new"),
        col("planid-new"),
        col("unitid-new"),
        col("ideaid-new"),
        col("adslotid-new"),
        col("city-new"),
        col("adclass-new"),
        col("uid-new")
      )).select("sample")

    sample.write.mode("overwrite").text(s"/user/cpc/dnn-sample/train")
    sample.write.mode("overwrite").text(s"/user/cpc/dnn-sample/test")

  }


  def genIntMap(spark: SparkSession, map: Map[Int, Int], name: String, dateList: List[String]): Unit = {
    println(name)
    for (date <- dateList) {
      val path = "/user/cpc/lrmodel/feature_ids_v1/%s/%s".format(name, date)
      try {
        val old = spark.read.parquet(path).rdd.map(x => x.getInt(0)).collect()
        for (row <- old) {
          if (!map.contains(row)) {
            currentMaxIdx += 1
            map(row) = currentMaxIdx
          }
        }
      } catch {
        case e: Exception =>
      }
    }
    println(s"finish $name map")
    println(map)
  }

  def getStrMapByDataset(spark: SparkSession, map: Map[String, Int], colName: String, dataset: Dataset[Row]): Unit = {
    println(colName)
    val res = dataset.select(colName).distinct().collect()
    for (row <- res) {
      val id = row(0).toString
      if (!map.contains(id)) {
        currentMaxIdx += 1
        map(id) = currentMaxIdx
      }
    }
    println(s"finish $colName map")
  }


}
