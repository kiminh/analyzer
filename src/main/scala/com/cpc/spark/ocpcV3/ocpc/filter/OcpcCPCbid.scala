package com.cpc.spark.ocpcV3.ocpc.filter

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpcV3.ocpc.OcpcUtils.getTimeRangeSql3
import com.typesafe.config.ConfigFactory
import ocpcCpcBid.ocpccpcbid.{OcpcMinBidList, SingleOcpcMinBid}
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

    val cvrDataRaw = getCvrData(date, hour, spark)
    cvrDataRaw.show(10)

    val conversionGoal = getConversioGoal(date, hour, spark)
    val cvrData = cvrDataRaw
      .join(conversionGoal, Seq("unitid"), "left_outer")
      .select("unitid", "cvr1", "cvr2", "cvr3", "conversion_goal")
      .withColumn("post_cvr", when(col("conversion_goal") === 1, col("cvr1")).otherwise(when(col("conversion_goal") === 2, col("cvr2")).otherwise(when(col("conversion_goal") === 3, col("cvr3")).otherwise(col("cvr1")))))

    cvrData.write.mode("overwrite").saveAsTable("test.check_data_ocpc20190216a")

    val data = cpcData
        .join(cvrData, Seq("unitid"), "outer")
        .select("unitid", "min_bid", "post_cvr")

    data.write.mode("overwrite").saveAsTable("test.check_data_ocpc20190216b")

//    savePbPack(data, fileName)
  }

  def getConversioGoal(date: String, hour: String, spark: SparkSession) = {
    val url = "jdbc:mysql://rr-2zehhy0xn8833n2u5.mysql.rds.aliyuncs.com:3306/adv?useUnicode=true&characterEncoding=utf-8"
    val user = "adv_live_read"
    val passwd = "seJzIPUc7xU"
    val driver = "com.mysql.jdbc.Driver"
    val table = "(select id, user_id, ideas, bid, ocpc_bid, ocpc_bid_update_time, cast(conversion_goal as char) as conversion_goal, status from adv.unit where is_ocpc=1 and ideas is not null) as tmp"

    val data = spark.read.format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("user", user)
      .option("password", passwd)
      .option("dbtable", table)
      .load()

    val base = data
      .withColumn("unitid", col("id"))
      .withColumn("userid", col("user_id"))
      .select("unitid", "userid", "ideas", "bid", "ocpc_bid", "ocpc_bid_update_time", "conversion_goal", "status")


    val ideaTable = base
      .withColumn("ideaid", explode(split(col("ideas"), "[,]")))
      .select("unitid", "userid", "ideaid", "ocpc_bid", "ocpc_bid_update_time", "conversion_goal", "status")

    ideaTable.createOrReplaceTempView("ideaid_update_time")

    val sqlRequest =
      s"""
         |SELECT
         |    unitid,
         |    userid,
         |    ideaid,
         |    ocpc_bid as cpa_given,
         |    cast(conversion_goal as int) as conversion_goal,
         |    ocpc_bid_update_time as update_timestamp,
         |    from_unixtime(ocpc_bid_update_time) as update_time,
         |    from_unixtime(ocpc_bid_update_time, 'yyyy-MM-dd') as update_date,
         |    from_unixtime(ocpc_bid_update_time, 'HH') as update_hour,
         |    status
         |FROM
         |    ideaid_update_time
       """.stripMargin

    println(sqlRequest)

    val rawData = spark.sql(sqlRequest)
    val resultDF = rawData.select("unitid", "conversion_goal")

    resultDF.show(10)
    resultDF
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
      .select("searchid", "unitid", "price", "isclick", "iscvr1", "iscvr2", "iscvr3")
      .groupBy("unitid")
      .agg(
        sum(col("isclick")).alias("click"),
        sum(col("iscvr1")).alias("cv1"),
        sum(col("iscvr2")).alias("cv2"),
        sum(col("iscvr3")).alias("cv3")
      )
      .withColumn("cvr1", col("cv1") * 1.0 / col("click"))
      .withColumn("cvr2", col("cv2") * 1.0 / col("click"))
      .withColumn("cvr3", col("cv3") * 1.0 / col("click"))

    data.show(10)
    data.write.mode("overwrite").saveAsTable("test.check_data_ocpc20190216c")
    data

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
    val selectCondition = getTimeRangeSql3(date1, hour1, date, hour)

    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  price,
         |  isclick
         |FROM
         |  dl_cpc.slim_union_log
         |WHERE
         |  $selectCondition
         |AND
         |  media_appsid in ('80000001', '80000002')
         |AND
         |  antispam = 0
         |AND
         |  adslot_type in (1,2,3)
         |AND
         |  adsrc=1
         |AND
         |  round(adclass/1000) != 132101  --去掉互动导流
         |AND
         |  isclick=1
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    data
  }

  def savePbPack(dataset: DataFrame, filename: String): Unit = {
    var list = new ListBuffer[SingleOcpcMinBid]
    println("size of the dataframe")
    println(dataset.count)
    dataset.show(10)
    dataset.printSchema()
    var cnt = 0

    for (record <- dataset.collect()) {
      val unit_id = record.getAs[String]("unitid").toLong
      val min_bid = record.getAs[Long]("min_bid").toDouble

      println(s"unit_id:$unit_id, min_bid:$min_bid")

      cnt += 1
      val currentItem = SingleOcpcMinBid(
        unitid = unit_id,
        minBid = min_bid
      )
      list += currentItem

    }

    val result = list.toArray[SingleOcpcMinBid]
    val adRecordList = OcpcMinBidList(
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
        min(col("min_bid")).alias("min_bid")
      )
      .select("unitid", "min_bid")

    resultDF

  }

}