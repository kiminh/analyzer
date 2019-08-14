package com.cpc.spark.oCPX.monitor

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.{getTimeRangeSqlDate, udfConcatStringInt, udfDetermineMedia}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OcpcUnitSurvivalMonitor {
  /*
  分媒体统计每个媒体下各个单元的日耗
  标记每个媒体下的单元数、近7天内新增单元数
   */
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString

    // 抽取基础表
    val baseData = getData(date, spark)
    val newUnits = getNewUnits(date, spark)

    // 存储基础数据
    baseData
      .withColumn("date", lit(date))
      .select("unitid", "userid", "media", "is_ocpc", "show", "click", "cost", "date")
      .repartition(5)
      .write.mode("overwrite").insertInto("dl_cpc.unit_cost_by_media_daily")

    // 统计近各个媒体，近七天的单元数、ocpc消费、非ocpc消费，新单元的消费数，近七天相对八天前有多少单元停止消费
    val data = calculateBaseData(newUnits, date, spark)

    // 存储基础数据
    data
      .withColumn("date", lit(date))
      .select("media", "is_ocpc", "unitid_cnt", "userid_cnt", "cost", "data_type", "date")
      .repartition(5)
      .write.mode("overwrite").insertInto("test.unit_changing_by_media_daily")


  }

  def calculateBaseData(newUnits: DataFrame, date: String, spark: SparkSession) = {
    // 统计近各个媒体，近七天的单元数、ocpc消费、非ocpc消费，新单元的消费数，近七天相对八天前有多少单元停止消费
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -7)
    val rawDate1 = calendar.getTime
    val date1 = dateConverter.format(rawDate1)
    calendar.add(Calendar.DATE, -7)
    val rawDate2 = calendar.getTime
    val date2 = dateConverter.format(rawDate2)

    val sqlRequest1 =
      s"""
         |SELECT
         |  unitid,
         |  userid,
         |  media,
         |  is_ocpc,
         |  sum(show) as show,
         |  sum(click) as click,
         |  sum(cost) * 0.01 as cost
         |FROM
         |  dl_cpc.unit_cost_by_media_daily
         |WHERE
         |  `date` >= '$date1'
         |GROUP BY unitid, userid, media, is_ocpc
       """.stripMargin
    println(sqlRequest1)
    val baseData = spark
      .sql(sqlRequest1)
      .withColumn("flag", lit(1))

    val sqlRequest2 =
      s"""
         |SELECT
         |  unitid,
         |  userid,
         |  media,
         |  is_ocpc,
         |  sum(show) as show,
         |  sum(click) as click,
         |  sum(cost) * 0.01 as cost
         |FROM
         |  dl_cpc.unit_cost_by_media_daily
         |WHERE
         |  `date` >= '$date2'
         |and
         |  `date` < '$date1'
         |GROUP BY unitid, userid, media, is_ocpc
       """.stripMargin
    println(sqlRequest2)
    val prevData = spark.sql(sqlRequest2)


    val newData = newUnits
      .withColumn("new_flag", col("flag"))
      .select("unitid",  "userid", "media", "is_ocpc")


    // 各个媒体，近七天的ocpc, 非ocpc单元数、ocpc消费、非ocpc消费
    val unitCnt = baseData
      .groupBy("media", "is_ocpc")
      .agg(
        countDistinct(col("unitid")).alias("unitid_cnt"),
        countDistinct(col("userid")).alias("userid_cnt"),
        sum(col("cost")).alias("cost")
      )
      .select("media", "is_ocpc", "unitid_cnt", "userid_cnt", "cost")
      .withColumn("data_type", lit("complete"))

    // 各个媒体，近七天新增的的ocpc, 非ocpc单元数、ocpc消费、非ocpc消费
    val newCnt = baseData
      .join(newData, Seq("unitid",  "userid", "media", "is_ocpc"), "inner")
      .groupBy("media", "is_ocpc")
      .agg(
        countDistinct(col("unitid")).alias("unitid_cnt"),
        countDistinct(col("userid")).alias("userid_cnt"),
        sum(col("cost")).alias("cost")
      )
      .select("media", "is_ocpc", "unitid_cnt", "userid_cnt", "cost")
      .withColumn("data_type", lit("new"))

    // 各个媒体上一个七天周期中停投单元数和消费
    val baseList = baseData
      .select("unitid",  "userid", "media", "is_ocpc", "flag")
    val lostCnt = prevData
      .join(baseList, Seq("unitid",  "userid", "media", "is_ocpc"), "left_outer")
      .na.fill(0, Seq("flag"))
      .filter(s"flag = 0")
      .groupBy("media", "is_ocpc")
      .agg(
        countDistinct(col("unitid")).alias("unitid_cnt"),
        countDistinct(col("userid")).alias("userid_cnt"),
        sum(col("cost")).alias("cost")
      )
      .select("media", "is_ocpc", "unitid_cnt", "userid_cnt", "cost")
      .withColumn("data_type", lit("lost"))

    val result = unitCnt.union(newCnt).union(lostCnt).cache()

    result.show()

    result
  }

  def getData(date: String, spark: SparkSession) = {
    // 取历史数据
    val selectCondition = s"`date` = '$date'"

    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias.total.media_selection"
    val mediaSelection = conf.getString(conf_key)
    // 取数据: score数据
    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  userid,
         |  media_appsid,
         |  is_ocpc,
         |  isshow,
         |  isclick,
         |  (case when isclick=1 then price else 0 end) as price
         |FROM
         |  dl_cpc.ocpc_base_unionlog
         |WHERE
         |  $selectCondition
         |AND
         |  $mediaSelection
       """.stripMargin
    println(sqlRequest)
    val clickData = spark
      .sql(sqlRequest)
      .withColumn("media", udfDetermineMedia()(col("media_appsid")))

    val resultDF = clickData
        .groupBy("unitid", "userid", "media", "is_ocpc")
        .agg(
          sum(col("isshow")).alias("show"),
          sum(col("isclick")).alias("click"),
          sum(col("price")).alias("cost")
        )
        .select("unitid", "userid", "media", "is_ocpc", "show", "click", "cost")
        .distinct()

    resultDF
  }

  def getNewUnits(date: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -7)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)

    val sqlRequest =
      s"""
         |SELECT
         |    unit_id as unitid,
         |    aduser as userid,
         |    target_medias,
         |    a as media_appsid,
         |    is_ocpc,
         |    create_time,
         |    date(create_time) as create_date
         |from
         |    qttdw.dim_unit_ds
         |lateral view explode(split(target_medias, ',')) b as a
         |WHERE
         |    dt = '$date'
         |and
         |    target_medias != ''
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)
      .select("unitid",  "userid", "media_appsid", "is_ocpc", "create_time", "create_date")
      .withColumn("media", udfDetermineMedia()(col("media_appsid")))

    val resultDF = data
        .filter(s"create_date > '$date1'")
        .withColumn("flag", lit(1))
        .select("unitid",  "userid", "media", "is_ocpc", "flag")
        .distinct()


    resultDF.show(10)
    resultDF
  }



}



