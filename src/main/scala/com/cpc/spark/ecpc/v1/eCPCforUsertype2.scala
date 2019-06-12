package com.cpc.spark.ecpc.v1

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import adclassEcpc.adclassEcpc.{SingleItem, AdClassEcpcList}
import scala.collection.mutable.ListBuffer

object eCPCforUsertype2 {
  def main(args: Array[String]): Unit = {
    /*
    计算新版的cvr平滑策略：
    1. 抽取基础数据
    2. 计算该维度下pcoc与计费比、后验cvr等等指标
    3. 计算该维度下根据给定highBidFactor计算出的lowBidFactor
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val media = args(3).toString
    val hourInt = args(4).toInt
    val highBidFactor = args(5).toDouble
    val minCV = args(6).toInt
    val fileName = "adclass_ecpc_v1.pb"

    println("parameters:")
    println(s"date=$date, hour=$hour, version:$version, media:$media, hourInt:$hourInt, highBidFactor:$highBidFactor, minCV:$minCV")

    // 抽取基础数据
    val rawData = getBaseData(media, hourInt, date, hour, spark)
    val baseData = rawData
      .withColumn("adtype", udfAdtypeMap()(col("adtype")))
      .withColumn("slottype", udfSlottypeMap()(col("slottype")))
      .select("searchid", "unitid", "is_api_callback", "adclass", "adtype", "slottype", "slotid", "bid", "price", "exp_cvr", "isshow", "isclick", "iscvr")

    // 计算各维度下的pcoc、jfb以及后验cvr等指标
    val data = calculateData(baseData, minCV, date, hour, spark)

//    string key = 1;
//    double post_cvr = 2;
//    double calCvrFactor = 3;
//    double highBidFactor = 4;
//    double lowBidFactor = 5;

    val resultDF = data
      .select("unitid", "is_api_callback", "post_cvr", "pcoc")
      .withColumn("high_bid_factor", lit(1.0))
      .withColumn("low_bid_factor", lit(1.0))

    resultDF
      .repartition(10).write.mode("overwrite").saveAsTable("test.check_elds_ecpc_data20190612")

//    savePbPack(resultDF, fileName, version, date, hour, spark)

  }

  def udfSelectPostCvr() = udf((postCvr1: Double, postCvr2: Double, postCvr3: Double, postCvr4: Double) => {
    var postCvr = 0.0
    if (postCvr1 > 0.0) {
      postCvr = postCvr1
    } else if (postCvr2 > 0.0) {
      postCvr = postCvr2
    } else if (postCvr3 > 0.0) {
      postCvr = postCvr3
    } else {
      postCvr = postCvr4
    }
    postCvr
  })

  def udfSelectPCOC() = udf((pcoc1: Double, pcoc2: Double, pcoc3: Double, pcoc4: Double) => {
    var pcoc = 0.0
    if (pcoc1 > 0.0) {
      pcoc = pcoc1
    } else if (pcoc2 > 0.0) {
      pcoc = pcoc2
    } else if (pcoc3 > 0.0) {
      pcoc = pcoc3
    } else {
      pcoc = pcoc4
    }

    pcoc
  })

  def calculateData(baseData: DataFrame, minCv: Int, date: String, hour: String, spark: SparkSession) = {
    baseData.createOrReplaceTempView("base_data")

    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  is_api_callback,
         |  sum(iscvr) * 1.0 / sum(isclick) as post_cvr,
         |  sum(case when isclick=1 then exp_cvr else 0 end) * 1.0 / sum(isclick) as pre_cvr,
         |  sum(case when isclick=1 then price else 0 end) * 1.0 / sum(isclick) as acp,
         |  sum(case when isclick=1 then bid else 0 end) * 1.0 / sum(isclick) as acb,
         |  sum(isclick) as click,
         |  sum(iscvr) as cv
         |FROM
         |  base_data
         |GROUP BY unitid, is_api_callback
       """.stripMargin
    println(sqlRequest)
    val data = spark
      .sql(sqlRequest)
      .withColumn("pcoc", col("pre_cvr") * 1.0 / col("post_cvr"))
      .withColumn("jfb", col("acp") * 1.0 / col("acb"))
      .select("unitid", "is_api_callback", "post_cvr", "pre_cvr", "acp", "acb", "pcoc", "jfb", "click", "cv")
      .filter(s"cv >= $minCv")

    data
  }

  def savePbPack(dataset: DataFrame, filename: String, version: String, date: String, hour: String, spark: SparkSession): Unit = {
    /*
    string key = 1;
    double post_cvr = 2;
    double calCvrFactor = 3;
    double highBidFactor = 4;
    double lowBidFactor = 5;
     */
    var list = new ListBuffer[SingleItem]
    dataset.createOrReplaceTempView("raw_data")
    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  is_api_callback,
         |  post_cvr,
         |  1.0 / pcoc as cvr_cal_factor,
         |  high_bid_factor,
         |  low_bid_factor
         |FROM
         |  raw_data
       """.stripMargin
    println(sqlRequest)
    val resultData = spark
      .sql(sqlRequest)
      .selectExpr("cast(unitid as bigint) unitid", "cast(is_api_callback as int) is_api_callback", "cast(post_cvr as double) post_cvr", "cast(cvr_cal_factor as double) cvr_cal_factor", "cast(high_bid_factor as double) high_bid_factor", "cast(low_bid_factor as double) low_bid_factor")
      .filter(s"cvr_cal_factor > 0 and post_cvr > 0")
      .cache()

    println("size of the dataframe:")
    println(resultData.count)
    resultData.show(10)
    resultData.printSchema()
    resultData
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
//      .repartition(10).write.mode("overwrite").saveAsTable("test.check_ecpc_pb_data")
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.adclass_ecpc_hourly")
    var cnt = 0

    for (record <- resultData.collect()) {
      val unitid = record.getAs[Long]("unitid").toString
      val isApiCallback = record.getAs[Int]("is_api_callback").toString
      val postCvr = record.getAs[Double]("post_cvr")
      val cvrCalFactor = record.getAs[Double]("cvr_cal_factor")
      val highBidFactor = record.getAs[Double]("high_bid_factor")
      val lowBidFactor = record.getAs[Double]("low_bid_factor")
      val key = unitid + "&" + isApiCallback

      if (cnt % 100 == 0) {
        println(s"key:$key, postCvr:$postCvr, cvrCalFactor:$cvrCalFactor, highBidFactor:$highBidFactor, lowBidFactor:$lowBidFactor")
      }
      cnt += 1

      val currentItem = SingleItem(
        key = key,
        postCvr = postCvr,
        calCvrFactor = cvrCalFactor,
        highBidFactor = highBidFactor,
        lowBidFactor = lowBidFactor
      )
      list += currentItem

    }

    val result = list.toArray[SingleItem]
    val adRecordList = AdClassEcpcList(
      records = result
    )

    println("length of the array")
    println(result.length)
    adRecordList.writeTo(new FileOutputStream(filename))

    println("complete save data into protobuffer")

  }

  def udfSlottypeMap() = udf((slottype: Int) => {
    /*
    tag name log as
    adslottype 列表页 1 1
    adslottype 详情页 2 2
    adslottype 互动页 3 3
    adslottype 开屏 4 4
    adslottype 横幅 5 14
    adslottype 视频 6 12
    adslottype 激励 7 9
     */
    val result = slottype match {
      case 1 => 1
      case 2 => 2
      case 3 => 3
      case 4 => 4
      case 5 => 14
      case 6 => 12
      case 7 => 9
      case _ => 0
    }
    result
  })

  def udfAdtypeMap() = udf((adtype: Int) => {
    /*
    adtype 文本 1 1
    adtype 大图 2 2
    adtype 图文 3 5
    adtype 组图 4 8
    adtype 互动 5 9
    adtype 开屏 6 10
    adtype 横幅 7 11
    adtype 横版视频 8 4
    adtype 激励 9 12
    adtype 竖版视频 10 13
     */
    val result = adtype match {
      case 1 => 1
      case 2 => 2
      case 3 => 5
      case 4 => 8
      case 5 => 9
      case 6 => 10
      case 7 => 11
      case 8 => 4
      case 9 => 12
      case 10 => 13
      case _ => 0
    }
    result
  })


  def getBaseData(media: String, hourInt: Int, date: String, hour: String, spark: SparkSession) = {
    // 抽取媒体id
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias." + media + ".media_selection"
    val mediaSelection = conf.getString(conf_key)

    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -hourInt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)

    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  adclass,
         |  adtype,
         |  adslot_type as slottype,
         |  adslotid as slotid,
         |  bid_discounted_by_ad_slot as bid,
         |  price,
         |  exp_cvr,
         |  unitid,
         |  (case
         |        when (cast(adclass as string) like '134%' or cast(adclass as string) like '107%') then "elds"
         |        when (adslot_type<>7 and cast(adclass as string) like '100%') then "feedapp"
         |        when (adslot_type=7 and cast(adclass as string) like '100%') then "yysc"
         |        when adclass = 110110100 then "wz"
         |        else "others"
         |    end) as industry,
         |  is_api_callback,
         |  isshow,
         |  isclick
         |FROM
         |  dl_cpc.ocpc_base_unionlog
         |WHERE
         |  $selectCondition
         |AND
         |  $mediaSelection
         |AND
         |  price <= bid_discounted_by_ad_slot
         |AND
         |  is_ocpc = 0
       """.stripMargin
    println(sqlRequest)
    val clickData = spark
      .sql(sqlRequest)
      .filter(s"industry in ('feedapp', 'wz')")
      .withColumn("conversion_goal", udfDecideConversionGoal()(col("industry"), col("is_api_callback")))

    val sqlRequest1 =
      s"""
         |SELECT
         |  searchid,
         |  label as iscvr1
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  `date` >= '$date1'
         |AND
         |  cvr_goal = 'cvr1'
       """.stripMargin
    val cvrData1 = spark.sql(sqlRequest1)

    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  label as iscvr2
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  `date` >= '$date1'
         |AND
         |  cvr_goal = 'cvr2'
       """.stripMargin
    val cvrData2 = spark.sql(sqlRequest2)

    val sqlRequest3 =
      s"""
         |SELECT
         |  searchid,
         |  label as iscvr3
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  `date` >= '$date1'
         |AND
         |  cvr_goal = 'cvr3'
       """.stripMargin
    val cvrData3 = spark.sql(sqlRequest3)

    val sqlRequest4 =
      s"""
         |SELECT
         |  searchid,
         |  label as iscvr4
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  `date` >= '$date1'
         |AND
         |  cvr_goal = 'cvr4'
       """.stripMargin
    val cvrData4 = spark.sql(sqlRequest4)

    val data = clickData
      .join(cvrData1, Seq("searchid"), "left_outer")
      .join(cvrData2, Seq("searchid"), "left_outer")
      .join(cvrData3, Seq("searchid"), "left_outer")
      .join(cvrData4, Seq("searchid"), "left_outer")
      .na.fill(0, Seq("iscvr1", "iscvr2", "iscvr3", "iscvr4"))
      .filter(s"conversion_goal > 0")
      .withColumn("iscvr", udfSelectCv()(col("conversion_goal"), col("iscvr1"), col("iscvr2"), col("iscvr3"), col("iscvr4")))

//    data
//      .repartition(100).write.mode("overwrite").saveAsTable("test.check_ecpc_data20190610")

    data
  }

  def udfDecideConversionGoal() = udf((industry: String, isApiCallback: Int) => {
    var cvGoal= 1
    if (industry == "feedapp" && isApiCallback == 1) {
      cvGoal = 2
    } else if (industry == "elds") {
      cvGoal = 3
    } else if (industry == "wz") {
      cvGoal = 4
    } else {
      cvGoal = 1
    }
    cvGoal
  })

  def udfSelectCv() = udf((conversionGoal: Int, iscvr1: Int, iscvr2: Int, iscvr3: Int, iscvr4: Int) => {
    val iscvr = conversionGoal match {
      case 1 => iscvr1
      case 2 => iscvr2
      case 3 => iscvr3
      case 4 => iscvr4
      case _ => 0
    }
    iscvr
  })


}
