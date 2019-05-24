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
    val fileName = "adclass_ecpc.pb"

    println("parameters:")
    println(s"date=$date, hour=$hour, version:$version, media:$media, hourInt:$hourInt")

    // 抽取基础数据
    val rawData = getBaseData(media, hourInt, date, hour, spark)
    val baseData = rawData
      .withColumn("adtype", udfAdtypeMap()(col("adtype")))
      .withColumn("slottype", udfSlottypeMap()(col("slottype")))
      .select("searchid", "adclass", "adtype", "slottype", "slotid", "bid", "price", "exp_cvr", "isshow", "isclick", "iscvr")

    val dataMain = baseData
      .select("adclass", "adtype", "slottype", "slotid")
      .distinct()

    // 计算各维度下的pcoc、jfb以及后验cvr等指标
    val data1 = calculateData1(baseData, 20, date, hour, spark)
//    data1.write.mode("overwrite").saveAsTable("test.check_ecpc_for_elds20190514a")
    val data2 = calculateData2(baseData, 20, date, hour, spark)
//    data2.write.mode("overwrite").saveAsTable("test.check_ecpc_for_elds20190514b")
    val data3 = calculateData3(baseData, 20, date, hour, spark)
//    data3.write.mode("overwrite").saveAsTable("test.check_ecpc_for_elds20190514c")
    val data4 = calculateData4(baseData, 20, date, hour, spark)
//    data4.write.mode("overwrite").saveAsTable("test.check_ecpc_for_elds20190514d")

    val data = dataMain
      .join(data1, Seq("adclass", "adtype", "slottype", "slotid"), "left_outer")
      .join(data2, Seq("adclass", "adtype", "slottype"), "left_outer")
      .join(data3, Seq("adclass", "adtype"), "left_outer")
      .join(data4, Seq("adclass"), "left_outer")
      .select("adclass", "adtype", "slottype", "slotid", "post_cvr1", "pcoc1", "post_cvr2", "pcoc2", "post_cvr3", "pcoc3", "post_cvr4", "pcoc4")
      .na.fill(0.0, Seq("post_cvr1", "pcoc1", "post_cvr2", "pcoc2", "post_cvr3", "pcoc3", "post_cvr4", "pcoc4"))
      .withColumn("post_cvr", udfSelectPostCvr()(col("post_cvr1"), col("post_cvr2"), col("post_cvr3"), col("post_cvr4")))
      .withColumn("pcoc", udfSelectPCOC()(col("pcoc1"), col("pcoc2"), col("pcoc3"), col("pcoc4")))
      .withColumn("high_bid_factor", lit(1.0))
      .withColumn("low_bid_factor", lit(1.0))

//    data.write.mode("overwrite").saveAsTable("test.check_ecpc_for_elds20190514")


    val resultDF = data
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))

    resultDF
      .repartition(10).write.mode("overwrite").saveAsTable("test.check_elds_ecpc_data")

    savePbPack(resultDF, fileName, version, date, hour, spark)

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

  def calculateData4(baseData: DataFrame, minCv: Int, date: String, hour: String, spark: SparkSession) = {
    baseData.createOrReplaceTempView("base_data")

    val sqlRequest =
      s"""
         |SELECT
         |  adclass,
         |  sum(iscvr) * 1.0 / sum(isclick) as post_cvr,
         |  sum(case when isclick=1 then exp_cvr else 0 end) * 1.0 / sum(isclick) as pre_cvr,
         |  sum(case when isclick=1 then price else 0 end) * 1.0 / sum(isclick) as acp,
         |  sum(case when isclick=1 then bid else 0 end) * 1.0 / sum(isclick) as acb,
         |  sum(isclick) as click,
         |  sum(iscvr) as cv
         |FROM
         |  base_data
         |GROUP BY adclass
       """.stripMargin
    println(sqlRequest)
    val data = spark
      .sql(sqlRequest)
      .withColumn("pcoc", col("pre_cvr") * 1.0 / col("post_cvr"))
      .withColumn("jfb", col("acp") * 1.0 / col("acb"))
      .select("adclass", "post_cvr", "pre_cvr", "acp", "acb", "pcoc", "jfb", "click", "cv")
      .filter(s"cv >= $minCv")
      .withColumn("post_cvr4", col("post_cvr"))
      .withColumn("pcoc4", col("pcoc"))
      .select("adclass", "post_cvr4", "pcoc4")

    data
  }

  def calculateData3(baseData: DataFrame, minCv: Int, date: String, hour: String, spark: SparkSession) = {
    baseData.createOrReplaceTempView("base_data")

    val sqlRequest =
      s"""
         |SELECT
         |  adclass,
         |  adtype,
         |  sum(iscvr) * 1.0 / sum(isclick) as post_cvr,
         |  sum(case when isclick=1 then exp_cvr else 0 end) * 1.0 / sum(isclick) as pre_cvr,
         |  sum(case when isclick=1 then price else 0 end) * 1.0 / sum(isclick) as acp,
         |  sum(case when isclick=1 then bid else 0 end) * 1.0 / sum(isclick) as acb,
         |  sum(isclick) as click,
         |  sum(iscvr) as cv
         |FROM
         |  base_data
         |GROUP BY adclass, adtype
       """.stripMargin
    println(sqlRequest)
    val data = spark
      .sql(sqlRequest)
      .withColumn("pcoc", col("pre_cvr") * 1.0 / col("post_cvr"))
      .withColumn("jfb", col("acp") * 1.0 / col("acb"))
      .select("adclass", "adtype", "post_cvr", "pre_cvr", "acp", "acb", "pcoc", "jfb", "click", "cv")
      .filter(s"cv >= $minCv")
      .withColumn("post_cvr3", col("post_cvr"))
      .withColumn("pcoc3", col("pcoc"))
      .select("adclass", "adtype", "post_cvr3", "pcoc3")


    data
  }

  def calculateData2(baseData: DataFrame, minCv: Int, date: String, hour: String, spark: SparkSession) = {
    baseData.createOrReplaceTempView("base_data")

    val sqlRequest =
      s"""
         |SELECT
         |  adclass,
         |  adtype,
         |  slottype,
         |  sum(iscvr) * 1.0 / sum(isclick) as post_cvr,
         |  sum(case when isclick=1 then exp_cvr else 0 end) * 1.0 / sum(isclick) as pre_cvr,
         |  sum(case when isclick=1 then price else 0 end) * 1.0 / sum(isclick) as acp,
         |  sum(case when isclick=1 then bid else 0 end) * 1.0 / sum(isclick) as acb,
         |  sum(isclick) as click,
         |  sum(iscvr) as cv
         |FROM
         |  base_data
         |GROUP BY adclass, adtype, slottype
       """.stripMargin
    println(sqlRequest)
    val data = spark
      .sql(sqlRequest)
      .withColumn("pcoc", col("pre_cvr") * 1.0 / col("post_cvr"))
      .withColumn("jfb", col("acp") * 1.0 / col("acb"))
      .select("adclass", "adtype", "slottype", "post_cvr", "pre_cvr", "acp", "acb", "pcoc", "jfb", "click", "cv")
      .filter(s"cv >= $minCv")
      .withColumn("post_cvr2", col("post_cvr"))
      .withColumn("pcoc2", col("pcoc"))
      .select("adclass", "adtype", "slottype", "post_cvr2", "pcoc2")

    data
  }

  def savePbPack(dataset: DataFrame, filename: String, version: String, date: String, hour: String, spark: SparkSession): Unit = {
    /*
    int64 adclass = 1;
    int64 adtype = 2;
    int64 slottype = 3;
    string slotid = 4;
    double post_cvr = 5;
    double calCvrFactor = 6;
    double highBidFactor = 7;
    double lowBidFactor = 8;
     */
    var list = new ListBuffer[SingleItem]
    dataset.createOrReplaceTempView("raw_data")
    val sqlRequest =
      s"""
         |SELECT
         |  adclass,
         |  adtype,
         |  slottype,
         |  slotid,
         |  1.0 / pcoc as cvr_cal_factor,
         |  post_cvr,
         |  high_bid_factor,
         |  low_bid_factor
         |FROM
         |  raw_data
       """.stripMargin
    println(sqlRequest)
    val resultData = spark
      .sql(sqlRequest)
      .selectExpr("cast(adclass as bigint) adclass", "cast(adtype as bigint) adtype", "cast(slottype as bigint) slottype", "cast(slotid as string) slotid", "cast(cvr_cal_factor as double) cvr_cal_factor", "cast(post_cvr as double) post_cvr", "cast(high_bid_factor as double) high_bid_factor", "cast(low_bid_factor as double) low_bid_factor")
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
      .repartition(10).write.mode("overwrite").saveAsTable("test.check_ecpc_pb_data")
//      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_calibration_v2_pb_hourly")
    var cnt = 0

    for (record <- resultData.collect()) {
      val adclass = record.getAs[Long]("adclass")
      val adtype = record.getAs[Long]("adtype")
      val slottype = record.getAs[Long]("slottype")
      val slotid = record.getAs[String]("slotid")
      val postCvr = record.getAs[Double]("post_cvr")
      val cvrCalFactor = record.getAs[Double]("cvr_cal_factor")
      val highBidFactor = record.getAs[Double]("high_bid_factor")
      val lowBidFactor = record.getAs[Double]("low_bid_factor")

      if (cnt % 100 == 0) {
        println(s"adclass:$adclass, adtype:$adtype, slottype:$slottype, slotid:$slotid, postCvr:$postCvr, cvrCalFactor:$cvrCalFactor, highBidFactor:$highBidFactor, lowBidFactor:$lowBidFactor")
      }
      cnt += 1

      val currentItem = SingleItem(
        adclass = adclass,
        adtype = adtype,
        slottype = slottype,
        slotid = slotid,
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

  def calculateData1(baseData: DataFrame, minCv: Int, date: String, hour: String, spark: SparkSession) = {
    /*
    int64 adclass = 1;
    int64 adtype = 2;
    int64 slottype = 3;
    string slotid = 4;
    double post_cvr = 5;
    double calCvrFactor = 6;
    double highBidFactor = 7;
    double lowBidFactor = 8;
     */
    baseData.createOrReplaceTempView("base_data")

    val sqlRequest =
      s"""
         |SELECT
         |  adclass,
         |  adtype,
         |  slottype,
         |  slotid,
         |  sum(iscvr) * 1.0 / sum(isclick) as post_cvr,
         |  sum(case when isclick=1 then exp_cvr else 0 end) * 1.0 / sum(isclick) as pre_cvr,
         |  sum(case when isclick=1 then price else 0 end) * 1.0 / sum(isclick) as acp,
         |  sum(case when isclick=1 then bid else 0 end) * 1.0 / sum(isclick) as acb,
         |  sum(isclick) as click,
         |  sum(iscvr) as cv
         |FROM
         |  base_data
         |GROUP BY adclass, adtype, slottype, slotid
       """.stripMargin
    println(sqlRequest)
    val data = spark
      .sql(sqlRequest)
      .withColumn("pcoc", col("pre_cvr") * 1.0 / col("post_cvr"))
      .withColumn("jfb", col("acp") * 1.0 / col("acb"))
      .select("adclass", "adtype", "slottype", "slotid", "post_cvr", "pre_cvr", "acp", "acb", "pcoc", "jfb", "click", "cv")
      .filter(s"cv >= $minCv")
      .withColumn("post_cvr1", col("post_cvr"))
      .withColumn("pcoc1", col("pcoc"))
      .select("adclass", "adtype", "slottype", "slotid", "post_cvr1", "pcoc1")

    data
  }

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
         |  (case
         |      when (cast(adclass as string) like '134%' or cast(adclass as string) like '107%') then 3
         |      when (adslot_type<>7 and cast(adclass as string) like '100%' and is_api_callback=1) then 2
         |      when (adslot_type<>7 and cast(adclass as string) like '100%' and is_api_callback!=1) then 1
         |      when (adclass = 110110100) then 1
         |      else 0
         |  end) as conversion_goal,
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
    val clickData = spark.sql(sqlRequest)

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

    val data = clickData
      .join(cvrData1, Seq("searchid"), "left_outer")
      .join(cvrData2, Seq("searchid"), "left_outer")
      .join(cvrData3, Seq("searchid"), "left_outer")
      .na.fill(0, Seq("iscvr1", "iscvr2", "iscvr3"))
      .filter(s"conversion_goal > 0")
      .withColumn("iscvr", udfSelectCv()(col("conversion_goal"), col("iscvr1"), col("iscvr2"), col("iscvr3")))

    data
  }

  def udfSelectCv() = udf((conversionGoal: Int, iscvr1: Int, iscvr2: Int, iscvr3: Int) => {
    val iscvr = conversionGoal match {
      case 1 => iscvr1
      case 2 => iscvr2
      case 3 => iscvr3
      case _ => 0
    }
    iscvr
  })


}
