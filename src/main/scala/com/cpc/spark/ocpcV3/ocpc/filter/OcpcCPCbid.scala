package com.cpc.spark.ocpcV3.ocpc.filter

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpcV3.ocpc.OcpcUtils._
import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import com.typesafe.config.ConfigFactory
import ocpcCpcBid.Ocpccpcbid
import ocpcCpcBid.ocpccpcbid.{OcpcCpcBidList, SingleOcpcCpcBid}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer


object OcpcCPCbid {
  def main(args: Array[String]): Unit = {
    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    // spark app name
    val spark = SparkSession.builder().appName(s"OcpcMinBid: $date, $hour").enableHiveSupport().getOrCreate()

    val conf = ConfigFactory.load("ocpc")
    val expDataPath = conf.getString("ocpc_all.ocpc_cpcbid.path")
    val fileName = conf.getString("ocpc_all.ocpc_cpcbid.pbfile")
    println(s"path is: $expDataPath")
    println(s"fileName is: $fileName")

    val cpcData = getExpData(expDataPath, date, hour, spark)
    cpcData.show(10)

    val cvrData = getCvrData(date, hour, spark)
    cvrData.show(10)

    val data = cpcData
        .join(cvrData, Seq("unitid"), "outer")
        .select("unitid", "min_bid1", "cvr1", "cvr2", "cvr3", "min_bid2", "min_cpm2")
        .withColumn("cvr1", when(col("unitid") === "270", 0.5).otherwise(col("cvr1")))
        .withColumn("cvr2", when(col("unitid") === "270", 0.5).otherwise(col("cvr2")))
        .withColumn("cvr3", when(col("unitid") === "270", 0.5).otherwise(col("cvr3")))
        .withColumn("min_bid", when(col("min_bid1").isNotNull, col("min_bid1")).otherwise(col("min_bid2")))
        .withColumn("min_cpm", col("min_cpm2"))
        .na.fill(0, Seq("min_bid", "cvr1", "cvr2", "cvr3", "min_cpm"))

    data
        .select("unitid", "min_bid", "cvr1", "cvr2", "cvr3", "min_cpm")
        .withColumn("date", lit(date))
        .withColumn("hour", lit(hour))
        .withColumn("version", lit("qtt_demo"))
        // .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_post_cvr_unitid_hourly20190218")
       .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_post_cvr_unitid_hourly")

    savePbPack(data, fileName)
  }


  def getCvrData(date: String, hour: String, spark: SparkSession) = {
    val clickData = getClickData(date, hour, spark)
    val cvr1Data = getCV("cvr1", date, hour, spark)
    val cvr2Data = getCV("cvr2", date, hour, spark)
    val cvr3Data = getCV("cvr3", date, hour, spark)

    val data = clickData
      .join(cvr1Data, Seq("searchid"), "left_outer")
      .join(cvr2Data, Seq("searchid"), "left_outer")
      .join(cvr3Data, Seq("searchid"), "left_outer")
      .select("searchid", "unitid", "price", "isclick", "iscvr1", "iscvr2", "iscvr3", "isshow", "bid", "exp_ctr")
      .withColumn("cpm", col("bid") * col("exp_ctr"))

    // data.createOrReplaceTempView("base_data")
    data.write.mode("overwrite").saveAsTable("test.ocpc_check_data_smooth")

    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  2 as min_bid2,
         |  sum(iscvr1) * 1.0 / sum(isclick) as cvr1,
         |  sum(iscvr2) * 1.0 / sum(isclick) as cvr2,
         |  sum(iscvr3) * 1.0 / sum(isclick) as cvr3,
         |  percentile(cpm, 0.15) as min_cpm2
         |FROM
         |  test.ocpc_check_data_smooth
         |GROUP BY unitid
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)

    resultDF.show(10)
    resultDF

  }

  def getCV(cvType: String, date: String, hour: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -72)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val colName = "is" + cvType

    val sqlRequest =
      s"""
         |SELECT
         |  searchid
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  `date` >= '$date1'
         |AND
         |  cvr_goal = '$cvType'
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest).withColumn(colName, lit(1)).select("searchid", colName)
    data
  }

  def getClickData(date: String, hour: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -72)
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
         |  unitid,
         |  price,
         |  isclick,
         |  isshow,
         |  (case when length(ocpc_log) > 0 then cast(ocpc_log_dict['dynamicbid'] as double)
         |        else original_bid end) as bid,
         |  exp_ctr * 1000000 as exp_ctr
         |FROM
         |  dl_cpc.filtered_union_log_exptag_hourly
         |WHERE
         |  $selectCondition
         |AND
         |  media_appsid in ('80000001', '80000002')
         |AND
         |  adslot_type in (1,2,3)
         |AND
         |  adsrc=1
         |AND
         |  round(adclass/1000) != 132101  --去掉互动导流
         |AND
         |  isshow=1
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    data
  }

  def savePbPack(dataset: DataFrame, filename: String): Unit = {
    var list = new ListBuffer[SingleOcpcCpcBid]
    println("size of the dataframe")
    println(dataset.count)
    dataset.show(10)
    dataset.printSchema()
    var cnt = 0

    for (record <- dataset.collect()) {
      val unit_id = record.getAs[String]("unitid").toLong
      val min_bid = record.getAs[Long]("min_bid").toDouble
      val post_cvr1 = record.getAs[Double]("cvr1")
      val post_cvr2 = record.getAs[Double]("cvr2")
      val post_cvr3 = record.getAs[Double]("cvr3")
      val min_cpm = record.getAs[Double]("min_cpm").toLong


      println(s"unit_id:$unit_id, min_bid:$min_bid, post_cvr1:$post_cvr1, post_cvr2:$post_cvr2, post_cvr3:$post_cvr3, min_cpm:$min_cpm")

      cnt += 1
      val currentItem = SingleOcpcCpcBid(
        unitid = unit_id,
        cpcBid = min_bid,
        cvGoal1PostCvr = post_cvr1,
        cvGoal2PostCvr = post_cvr2,
        cvGoal3PostCvr = post_cvr3,
        minCpm = min_cpm
      )
      list += currentItem

    }

    val result = list.toArray[SingleOcpcCpcBid]
    val adRecordList = OcpcCpcBidList(
      adrecord = result
    )

    println("length of the array")
    println(result.length)
    adRecordList.writeTo(new FileOutputStream(filename))

    println("complete save data into protobuffer")

  }

  def getExpData(expDataPath: String, date: String, hour: String, spark: SparkSession) = {
    val data = spark.read.format("json").json(expDataPath)

    val resultDF = data
      .groupBy("unitid")
      .agg(
        min(col("min_bid")).alias("min_bid1")
      )
      .select("unitid", "min_bid1")

    resultDF.show(10)
    resultDF

  }

}