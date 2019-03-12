package com.cpc.spark.OcpcProtoType.suggest_cpa_qtt

import java.text.SimpleDateFormat
import java.util.Calendar

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object OcpcGetCV {
  def main(args: Array[String]): Unit = {
    /*
    每个unitid在每个转化目标下面当天各自累积的转化数据
    按照日期抽取包含对应转化标签的每条记录的基础表，按照unitid统计各自的转化数
     */
    // sh test.sh 2019-03-12 12 qtt 1
    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val media = args(2).toString
    val conversionGoal = args(3).toInt
    val version = "qtt_demo"
    val spark = SparkSession
      .builder()
      .appName(s"ocpc get cv: $date, $hour")
      .enableHiveSupport().getOrCreate()

    println("parameters:")
    println(s"date=$date, hour=$hour, media=$media, conversionGoal=$conversionGoal, version=$version")

    // 计算
    val data = getBaseData(media, conversionGoal, date, hour, spark)

    // 增加分区字段
    val resultDF = data
      .withColumn("date", lit(date))
      .withColumn("conversion_goal", lit(conversionGoal))
      .withColumn("version", lit(version))

    resultDF
      .repartition(10).write.mode("overwrite").saveAsTable("test.check_unitid_cv_data")

  }

  def getBaseData(media: String, conversionGoal: Int, date: String, hour: String, spark: SparkSession) = {
    // 媒体选择
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias." + media + ".media_selection"
    val mediaSelection = conf.getString(conf_key)

    // 转化标签的cv_pt分区选择
    var cvrGoal = "cvr" + conversionGoal.toString

    // 日期
    // 时间分区
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)

    // 抽取点击数据表
    val sqlRequest1 =
      s"""
         |SELECT
         |    searchid,
         |    unitid,
         |    isclick,
         |    isshow
         |FROM
         |    dl_cpc.ocpc_base_unionlog
         |WHERE
         |    `date` = '$date1'
         |AND
         |    $mediaSelection
         |AND
         |    isclick=1
         |AND
         |    antispam = 0
         |AND
         |    adslot_type in (1,2,3)
         |AND
         |    adsrc = 1
         |AND
         |    (charge_type is null or charge_type = 1)
       """.stripMargin
    println(sqlRequest1)
    val ctrData = spark.sql(sqlRequest1)

    // 抽取转化数据
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  label as iscvr
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  `date` >= '$date1'
         |AND
         |  cvr_goal = '$cvrGoal'
       """.stripMargin
    println(sqlRequest2)
    val cvrData = spark.sql(sqlRequest2)

    // 数据关联
    val data = ctrData.join(cvrData, Seq("searchid"), "left_outer")

    // 数据统计
    val resultDF = data
      .groupBy("unitid")
      .agg(sum(col("iscvr")).alias("cv"))
      .select("unitid", "cv")

    resultDF.show(10)
    resultDF
  }
}
