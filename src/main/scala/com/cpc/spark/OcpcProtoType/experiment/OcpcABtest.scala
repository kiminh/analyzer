package com.cpc.spark.OcpcProtoType.experiment

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object OcpcABtest {
  def main(args: Array[String]): Unit = {
    /*
    identifier维度下的ab实验：提供cpcbid和是否开启ab实验的开关。bid的来源来自两个地方：在实验配置文件中提供，根据cpc阶段的历史出价数据进行计算。
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val media = args(2).toString
    val version = args(3).toString

    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version, media=$media")
    val data = readExpSet(date, hour, spark)

    val dataWithCPC = getCPCbid(media, version, date, hour, spark)
    dataWithCPC.show(10)

  }

  def getDurationByDay(date: String, hour: String, dayInt: Int, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -dayInt)
    val startdate = calendar.getTime
    val date1 = dateConverter.format(startdate)
    val selectCondition = getTimeRangeSql2(date1, hour, date, hour)

    selectCondition
  }

  def getCPCbid(media: String, version: String, date: String, hour: String, spark: SparkSession) = {
    /*
    根据cpc阶段的历史出价数据计算用于ab实验的bid：
    1. 计算最近三天的cpc出价
    2. 检查最近七天是否有ocpc展现记录
    3. 过滤掉最近七天有ocpc展现记录的广告，保留剩下的cpc出价记录
    4. 读取前一天的cpcbid出价表
    5. 以外关联的方式，将第三步得到的新表中的出价记录替换第四步中的对应的identifier的cpc出价，保存结果到新的时间分区
     */
    val conf_key = "medias." + media + ".media_selection"
    val conf = ConfigFactory.load("ocpc")
    val mediaSelection = conf.getString(conf_key)
    // 计算最近三天的cpc出价
    val selectCondition1 = getDurationByDay(date, hour, 3, spark)
    val sqlRequest1 =
      s"""
         |SELECT
         |  cast(unitid as string) as identifier,
         |  sum(case when isclick=1 then bid else 0 end) * 1.0 / sum(isclick) as bid
         |FROM
         |  dl_cpc.ocpc_base_unionlog
         |WHERE
         |  $selectCondition1
         |AND
         |  $mediaSelection
         |AND
         |  length(ocpc_log) > 0
         |GROUP BY cast(unitid as string)
       """.stripMargin
    println(sqlRequest1)
    val cpcData = spark.sql(sqlRequest1)

    // 检查最近七天是否有ocpc展现记录
    val selectCondition2 = getDurationByDay(date, hour, 7, spark)
    val sqlRequets2 =
      s"""
         |SELECT
         |  searchid,
         |  cast(unitid as string) as identifier
         |FROM
         |  dl_cpc.ocpc_filter_unionlog
         |WHERE
         |  $selectCondition2
         |AND
         |  is_ocpc=1
         |AND
         |  $mediaSelection
       """.stripMargin
    println(sqlRequets2)
    val ocpcRecord = spark
      .sql(sqlRequets2)
      .withColumn("is_ocpc", lit(1))
      .select("identifier", "is_ocpc")
      .distinct()

    // 过滤掉最近七天有ocpc展现记录的广告，保留剩下的cpc出价记录
    val joinData = cpcData
      .join(ocpcRecord, Seq("identifier"), "left_outer")
      .select("identifier", "bid", "is_ocpc")
      .na.fill(0, Seq("is_ocpc"))

    val cpcBid = joinData
      .filter(s"is_ocpc=0")
      .withColumn("current_bid", col("bid"))
      .select("identifier", "current_bid")

    // 读取前一天的cpcbid出价表
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val startdate = calendar.getTime
    val date1 = dateConverter.format(startdate)
    val selectCondition3 = s"date = '$date1' and version = '$version'"
    val sqlRequest3 =
      s"""
         |SELECT
         |  identifier,
         |  cpc_bid as prev_bid,
         |  duration as prev_duration
         |FROM
         |  dl_cpc.ocpc_cpc_bid_exp
         |WHERE
         |  $selectCondition3
       """.stripMargin
    println(sqlRequest3)
    val prevBid = spark.sql(sqlRequest3)

    // 以外关联的方式，将第三步得到的新表中的出价记录替换第四步中的对应的identifier的cpc出价，保存结果到新的时间分区
    val result = prevBid
      .join(cpcBid, Seq("identifier"), "outer")
      .select("identifier", "current_bid", "prev_bid", "prev_duration")
      .withColumn("is_update", when(col("current_bid").isNotNull, 1).otherwise(0))
      .withColumn("bid", when(col("is_update") === 1, col("current_bid")).otherwise(col("prev_bid")))
      .withColumn("duration", when(col("is_update") === 1, 1).otherwise(col("prev_duration") + 1))

    result.write.mode("overwrite").saveAsTable("test.check_ab_test20190203")

    val resultDF = result.select("identifier", "bid", "duration")
    resultDF
  }

  def readExpSet(date: String, hour: String, spark: SparkSession) = {
    val path = s"/user/cpc/wangjun/ocpc_exp/ocpc_ab.json"

    val data = spark.read.format("json").json(path)

    val resultDF = data
        .groupBy("identifier")
        .agg(
          min(col("cpc_bid")).alias("cpc_bid")
        )
        .select("identifier", "cpc_bid")

    resultDF
  }

}

