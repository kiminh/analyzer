package com.cpc.spark.OcpcProtoType.bs

import java.text.SimpleDateFormat
import java.util.Calendar
import scala.collection.mutable.ListBuffer
import java.io.FileOutputStream

import com.cpc.spark.OcpcProtoType.OcpcTools._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import ocpcBsParmas.ocpcBsParmas.{SingleItem, OcpcBsParmasList}


object OcpcBsData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val expTag = args(3).toString
    val hourInt = args(4).toInt
    val minCV = args(5).toInt
    val fileName = args(6).toString
    println("parameters:")
    println(s"date=$date, hour=$hour, hourInt:$hourInt, version:$version, expTag:$expTag, minCV:$minCV, fileName:$fileName")


    val baseData = getBaseData(hourInt, date, hour, spark)
    baseData
      .repartition(100)
      .write.mode("overwrite").saveAsTable("test.check_ocpc_data20190916b")

    // 计算结果
    val data = calculateData(baseData, expTag, spark)
    val result = data.filter(s"cv >= $minCV")

    result
        .withColumn("date", lit(date))
        .withColumn("hour", lit(hour))
        .withColumn("exp_tag", lit(expTag))
        .withColumn("version", lit(version))
        .repartition(5)
        .write.mode("overwrite").insertInto("test.ocpc_bs_params_pb_hourly")
//        .write.mode("overwrite").insertInto("dl_cpc.ocpc_bs_params_pb_hourly")


    savePbPack(result, fileName, spark)

  }

  def savePbPack(data: DataFrame, fileName: String, spark: SparkSession): Unit = {
    /*
    proto:
    message singleitem {
      string   key = 1;
      double   cvrcalfactor = 2;
      double   jfbfactor = 3;
      double   smoothfactor = 4;
      double   postcvr = 5;
      double   postctr = 6;
    }

    dataframe:
    ("key", "cv", "cvr", "ctr")
     */

    var list = new ListBuffer[SingleItem]
    var cnt = 0

    for (record <- data.collect()) {
      val key = record.getAs[String]("key")
      val postcvr = record.getAs[Double]("cvr")
      val postctr = record.getAs[Double]("ctr")
      val cvrFactor = record.getAs[Double]("cvr_factor")
      val jfbFactor = record.getAs[Double]("jfb_factor")

      if (cnt % 100 == 0) {
        println(s"key:$key, postcvr:$postcvr, postctr:$postctr, cvrFactor:$cvrFactor, jfbFactor:$jfbFactor")
      }
      cnt += 1

      val currentItem = SingleItem(
        key = key,
        cvrCalFactor = cvrFactor,
        jfbFactor = jfbFactor,
        smoothFactor = 0.0,
        postCvr = postcvr,
        postCtr = postctr
      )
      list += currentItem

    }
    val result = list.toArray[SingleItem]
    val adRecordList = OcpcBsParmasList(
      records = result
    )

    println("length of the array")
    println(result.length)
    adRecordList.writeTo(new FileOutputStream(fileName))

    println("complete save data into protobuffer")

  }


  def calculateData(baseData: DataFrame, expTag: String, spark: SparkSession) = {
    baseData.createOrReplaceTempView("base_data")
    val sqlRequest1 =
      s"""
         |SELECT
         |  unitid,
         |  media,
         |  sum(case when isclick=1 then iscvr else 0 end) as cv,
         |  sum(case when isclick=1 then iscvr else 0 end) * 1.0 / sum(isclick) as cvr,
         |  sum(isclick) * 1.0 / sum(isshow) as ctr,
         |  sum(case when isclick=1 and bscvr>0 then bscvr else 0 end) * 1.0 / sum(case when isclick=1 and bscvr > 0 then 1 else 0 end) as bscvr,
         |  sum(case when isclick=1 then price else 0 end) * 0.01 as total_price,
         |  sum(case when isclick=1 then bid else 0 end) * 0.01 as total_bid
         |FROM
         |  base_data
         |GROUP BY unitid, media
       """.stripMargin
    println(sqlRequest1)
    val data1 = spark
      .sql(sqlRequest1)
      .withColumn("exp_tag", lit(expTag))
      .withColumn("exp_tag", concat(col("exp_tag"), col("media")))
      .withColumn("key", concat_ws("&", col("exp_tag"), col("unitid")))
      .select("key", "cv", "cvr", "ctr", "bscvr")
      .withColumn("cvr_factor", col("cvr") * 1.0 / col("bscvr"))
      .withColumn("jfb_factor", col("total_bid") * 1.0 / col("total_price"))
      .na.fill(1.0, Seq("cvr_factor", "jfb_factor"))
      .select("key", "cv", "cvr", "ctr", "cvr_factor", "jfb_factor")
      .cache()

    val sqlRequest2 =
      s"""
         |SELECT
         |  media,
         |  adslot_type,
         |  adtype,
         |  conversion_goal,
         |  sum(case when isclick=1 then iscvr else 0 end) as cv,
         |  sum(case when isclick=1 then iscvr else 0 end) * 1.0 / sum(isclick) as cvr,
         |  sum(isclick) * 1.0 / sum(isshow) as ctr,
         |  sum(case when isclick=1 and bscvr > 0 then bscvr else 0 end) * 1.0 / sum(case when isclick=1 and bscvr > 0 then 1 else 0 end) as bscvr,
         |  sum(case when isclick=1 then price else 0 end) * 0.01 as total_price,
         |  sum(case when isclick=1 then bid else 0 end) * 0.01 as total_bid
         |FROM
         |  base_data
         |GROUP BY media, adslot_type, adtype, conversion_goal
       """.stripMargin
    println(sqlRequest2)
    val data2 = spark
      .sql(sqlRequest2)
      .withColumn("exp_tag", lit(expTag))
      .withColumn("exp_tag", concat(col("exp_tag"), col("media")))
      .withColumn("key", concat_ws("&", col("exp_tag"), col("adslot_type"), col("adtype"), col("conversion_goal")))
      .select("key", "cv", "cvr", "ctr", "bscvr")
      .withColumn("cvr_factor", col("cvr") * 1.0 / col("bscvr"))
      .withColumn("jfb_factor", col("total_bid") * 1.0 / col("total_price"))
      .na.fill(1.0, Seq("cvr_factor", "jfb_factor"))
      .select("key", "cv", "cvr", "ctr", "cvr_factor", "jfb_factor")
      .cache()

    data1.show(10)
    data2.show(10)

    val data = data1
      .union(data2)
      .selectExpr("key", "cv", "cast(cvr as double) cvr", "cast(ctr as double) ctr", "cast(cvr_factor as double) cvr_factor", "cast(jfb_factor as double) jfb_factor")


    data



  }

  def getBaseData(hourInt: Int, date: String, hour: String, spark: SparkSession) = {
    // 抽取媒体id
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias.total.media_selection"
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
    val selectCondition = getTimeRangeSqlDate(date1, hour1, date, hour)

    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  conversion_goal,
         |  adslot_type,
         |  adtype as original_adtype,
         |  isshow,
         |  isclick,
         |  (case
         |      when media_appsid in ('80000001', '80000002') then 'Qtt'
         |      when media_appsid in ('80002819', '80004944') then 'HT66'
         |      else 'MiDu'
         |  end) as media,
         |  cast(exp_cvr as double) as exp_cvr,
         |  cast(exp_ctr as double) as exp_ctr,
         |  cast(bscvr as double) * 1.0 / 1000000 as bscvr,
         |  bid_discounted_by_ad_slot as bid,
         |  price
         |FROM
         |  dl_cpc.ocpc_base_unionlog
         |WHERE
         |  $selectCondition
         |AND
         |  $mediaSelection
         |AND
         |  isshow = 1
         |AND
         |  is_ocpc = 1
       """.stripMargin
    println(sqlRequest)
    val clickData = spark
      .sql(sqlRequest)
      .withColumn("cvr_goal", udfConcatStringInt("cvr")(col("conversion_goal")))
      .withColumn("adtype", udfMapAdtype()(col("original_adtype")))

    // 抽取cv数据
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  label as iscvr,
         |  cvr_goal
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest2)
    val cvData = spark.sql(sqlRequest2)


    // 数据关联
    val resultDF = clickData
      .join(cvData, Seq("searchid", "cvr_goal"), "left_outer")
      .na.fill(0, Seq("iscvr"))

    resultDF
  }

  def udfMapAdtype() = udf((adType: Int) => {
    /*
    adtype 文本 1 1 6
    adtype 大图 2 2 2
    adtype 图文 3 5 1
    adtype 组图 4 8 3
    adtype 互动 5 9 7
    adtype 开屏 6 10 8
    adtype 横幅 7 11 9
    adtype 横版视频 8 4 4
    adtype 激励 9 12 10
    adtype 竖版视频 10 13 11
    adtype 激励视频 11    12
    adtype 激励竖版视频 15    112
     */
    var result = adType match {
      case 1 => 6
      case 2 => 2
      case 3 => 1
      case 4 => 3
      case 5 => 7
      case 6 => 8
      case 7 => 9
      case 8 => 4
      case 9 => 10
      case 10 => 11
      case 11 => 12
      case 15 => 112
      case _ => 0
    }
    result
  })

}