package com.cpc.spark.ml.ctrmodel.hourly


import java.io.FileOutputStream
import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.mutable.{ListBuffer, Map}
import com.cpc.spark.qukan.utils.Udfs._
import org.apache.spark.sql.functions.{col, concat_ws, lit, udf, when}
import com.cpc.spark.qukan.utils.SmallUtil
import mlmodel.mlmodel.DnnDict

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
    val dictPath = args(2)
    println(s"date=$date, hour=$hour, dictPath=$dictPath")
    val dateListBuffer = new ListBuffer[String]()

    val days = 1
    for (i <- 0 until days) {
      val newday = SmallUtil.getDayBefore(date, i)
      print("newday", newday)
      dateListBuffer.append(newday)
    }
    val dateList = dateListBuffer.toList


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
      s"""
         | select label,
         |   media_appsid as mediaid,
         |   planid, unitid, ideaid, adslotid,
         |   city, adclass, uid
         | from dl_cpc.ml_ctr_feature_v1
         | where `date` in ('${dateList.mkString("','")}') and
         | media_appsid in (80000001, 80000002)
         | and adslot_type in (1)
      """.stripMargin

    println(sql)

    val sample0 = spark.sql(sql)
      .limit(100000)
    getStrMapByDataset(spark, uidMap, "uid", sample0)
    val uidDataset = uidToDataset(spark, uidMap)

    print(s"uidDataset count = ${uidDataset.count()}")
    println(s"max index = $currentMaxIdx")
    println(s"sample count = ${sample0.count()}")

    for (i <- 0 until 1) {
      val newday = SmallUtil.getDayBefore(date, i)
      print("newday", newday)
      val sql1 =
        s"""
           | select label,
           |   media_appsid as mediaid,
           |   planid, unitid, ideaid, adslotid,
           |   city, adclass, uid
           | from dl_cpc.ml_ctr_feature_v1
           | where `date` in ('$newday') and
           | media_appsid in (80000001, 80000002)
           | and adslot_type in (1)
      """.stripMargin

      println("sql1", sql1)
      val sample = spark.sql(sql1)
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

      sample.write.mode("overwrite").text(s"/user/cpc/dnn-sample/train$i")
      // sample.repartition(1000).write.mode("overwrite").text(s"/user/cpc/dnn-sample/test$i")

      val pack = DnnDict(
        mediaIdDict = this.mediaIdMap.toMap,
        planIdDict = this.planIdMap.toMap,
        unitIdDict = unitIdMap.toMap,
        ideaIdDict = ideaIdMap.toMap,
        adslotIdDict = adslotIdMap.toMap,
        cityDict = cityMap.toMap,
        adclassDict = adclassMap.toMap,
        brandDict = brandMap.toMap,
        uidDict = uidMap.toMap
      )

      pack.writeTo(new FileOutputStream(dictPath))
    }

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

  def uidToDataset(spark: SparkSession, map: Map[String, Int]): Dataset[Row] = {
    var dataset: Dataset[Row] = null
    for ((k, v) <- map) {
      if (dataset == null) {
        dataset = spark.sql(s"select '$k' as uid, $v as uid_index")
      } else {
        dataset = dataset.union(spark.sql(s"select '$k' as uid, $v as uid_index"))
      }
    }
    dataset
  }

}
