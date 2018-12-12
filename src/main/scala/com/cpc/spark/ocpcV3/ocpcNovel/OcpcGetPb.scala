package com.cpc.spark.ocpcV3.ocpcNovel

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import ocpcnovel.ocpcnovel.SingleUnit
import ocpcnovel.ocpcnovel.OcpcNovelList
import org.apache.spark.sql.types.{DoubleType, IntegerType}
//import ocpcnovel.ocpcnovel

import scala.collection.mutable.ListBuffer


object OcpcGetPb {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    val cvrData = getCvr(date, hour, spark)
    val kvalue = getK(date, hour, spark)
    val cpaHistory = getCPAhistory(date, hour, spark)
    val adclassCPA = spark
      .table("dl_cpc.ocpcv3_adclass_cpa_history_hourly")
      .where(s"`date`='$date' and `hour`='$hour'")
      .select("new_adclass", "avg_cpa1", "avg_cpa2")

    // 检查cpa_history=0
    val data = cvrData
      .join(kvalue, Seq("unitid"), "left_outer")
      .select("unitid", "kvalue", "cvr1cnt", "cvr2cnt", "new_adclass")
      .withColumn("kvalue", when(col("kvalue").isNull, 0.0).otherwise(col("kvalue")))
      .join(cpaHistory, Seq("unitid"), "left_outer")
      .select("unitid", "cpa_history", "kvalue", "cvr1cnt", "cvr2cnt", "conversion_goal", "new_adclass")
      .withColumn("conversion_goal", when(col("conversion_goal").isNull && col("cvr2cnt")>0, 2).otherwise(1))
      .filter(s"conversion_goal is not null")
      .join(adclassCPA, Seq("new_adclass"), "left_outer")
      .select("unitid", "cpa_history", "kvalue", "cvr1cnt", "cvr2cnt", "conversion_goal", "new_adclass", "avg_cpa1", "avg_cpa2")
      .withColumn("avg_cpa", when(col("conversion_goal")===1, col("avg_cpa1")).otherwise(col("avg_cpa2")))
      .withColumn("cpa_history_old", col("cpa_history"))
      .withColumn("cpa_history", when(col("cpa_history").isNull || col("cpa_history") === -1, col("avg_cpa")).otherwise(col("cpa_history")))
      .withColumn("cpa_history", when(col("cpa_history") > 50000, 50000).otherwise(col("cpa_history")))
      .filter("cpa_history is not null and cpa_history>0 and kvalue>=0")
      .select("unitid", "cpa_history", "kvalue", "cvr1cnt", "cvr2cnt", "conversion_goal", "cpa_history_old", "avg_cpa", "avg_cpa1", "avg_cpa2", "new_adclass")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

//    data.write.mode("overwrite").saveAsTable("test.ocpcv3_novel_pb_v1_hourly_middle")
    data.write.mode("overwrite").insertInto("dl_cpc.ocpcv3_novel_pb_v1_hourly_middle")

//    unitid          int,
//    cpa_history     double,
//    kvalue          double,
//    cvr1cnt         bigint,
//    cvr2cnt         bigint,
//    conversion_goal int
    val result = data
      .select("unitid", "cpa_history", "kvalue", "cvr1cnt", "cvr2cnt", "conversion_goal")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    result.write.mode("overwrite").saveAsTable("test.ocpcv3_novel_pb_v1_hourly")
    // 原表名：dl_cpc.ocpcv3_novel_pb_hourly
    result.write.mode("overwrite").insertInto("dl_cpc.ocpcv3_novel_pb_v1_hourly")

    // 输出pb文件
    savePbPack(result)
  }

  def getK(date: String, hour: String, spark: SparkSession) = {
    // 先获取回归模型和备用模型的k值
    // 根据conversion_goal选择需要的k值
    val cpaHistory = spark
      .table("dl_cpc.ocpcv3_novel_cpa_history_hourly")
      .where(s"`date`='$date' and `hour`='$hour'")
      .filter(s"conversion_goal is not null")
      .select("unitid", "adclass", "conversion_goal")
      .distinct()

    val tableName1 = "dl_cpc.ocpc_v3_novel_k_regression"
    val rawData1 = spark
      .table(tableName1)
      .where(s"`date`='$date' and `hour`='$hour'")
      .select("unitid", "k_ratio1", "k_ratio2")
    rawData1.show(10)

    val tableName2 = "dl_cpc.ocpc_novel_k_value_table"
    val rawData2 = spark
      .table(tableName2)
      .where(s"`date`='$date' and `hour`='$hour'")
      .filter("conversion_goal is not null and k_value is not null")
      .select("unitid", "adclass", "k_value")
    rawData2.show(10)

    val data = cpaHistory
      .join(rawData1, Seq("unitid"), "left_outer")
      .join(rawData2, Seq("unitid", "adclass"), "left_outer")
      .select("unitid", "adclass", "k_value", "conversion_goal", "k_ratio1", "k_ratio2")
      .filter("adclass is not null and conversion_goal is not null")
      .withColumn("k_ratio", when(col("conversion_goal") === 2, col("k_ratio2")).otherwise(col("k_ratio1")))
      .withColumn("kvalue", when(col("k_ratio").isNull, col("k_value")).otherwise(col("k_ratio")))
      .filter(s"kvalue > 0 and kvalue is not null")
      .withColumn("kvalue", when(col("kvalue") > 5.0, 5.0).otherwise(col("kvalue")))
      .withColumn("kvalue", when(col("kvalue") < 0.1, 0.1).otherwise(col("kvalue")))

    val resultDF = data.select("unitid", "kvalue")
//    data.write.mode("overwrite").saveAsTable("test.ocpcv3_novel_kvalue_data_hourly")


    resultDF
  }

  def getCPAhistory(date: String, hour: String, spark: SparkSession) = {
    val tableName = "dl_cpc.ocpcv3_novel_cpa_history_hourly"
    val resultDF = spark
      .table(tableName)
      .where(s"`date`='$date' and `hour`='$hour'")
    resultDF.show(10)
//    resultDF.write.mode("overwrite").saveAsTable("test.ocpcv3_novel_cpa_data_hourly")

    resultDF
  }

  def getCvr(date: String, hour: String, spark: SparkSession) = {
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
    // ctr data
    val sqlRequestCtrData =
      s"""
         |SELECT
         |  unitid,
         |  adclass,
         |  ctr_cnt
         |FROM
         |  dl_cpc.ocpcv3_ctr_data_hourly
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequestCtrData)
    val ctrData = spark
      .sql(sqlRequestCtrData)
      .select("unitid", "adclass")
      .withColumn("new_adclass", col("adclass")/1000)
      .withColumn("new_adclass", col("new_adclass").cast(IntegerType))
      .select("unitid", "new_adclass")
      .distinct()

    // cvr data
    // cvr1 or cvr3 data
    val sqlRequestCvr1Data =
    s"""
       |SELECT
       |  unitid,
       |  cvr1_cnt
       |FROM
       |  dl_cpc.ocpcv3_cvr1_data_hourly
       |WHERE
       |  $selectCondition
       |AND
       |  media_appsid in ("80001098","80001292")
       """.stripMargin
    println(sqlRequestCvr1Data)
    val cvr1Data = spark
      .sql(sqlRequestCvr1Data)
      .groupBy("unitid")
      .agg(sum(col("cvr1_cnt")).alias("cvr1cnt"))
    cvr1Data.show(10)

    // cvr2data
    val sqlRequestCvr2Data =
      s"""
         |SELECT
         |  unitid,
         |  cvr2_cnt
         |FROM
         |  dl_cpc.ocpcv3_cvr2_data_hourly
         |WHERE
         |  $selectCondition
         |AND
         |  media_appsid in ("80001098","80001292")
       """.stripMargin
    println(sqlRequestCvr2Data)
    val cvr2Data = spark
      .sql(sqlRequestCvr2Data)
      .groupBy("unitid")
      .agg(sum(col("cvr2_cnt")).alias("cvr2cnt"))
    cvr2Data.show(10)

    // 数据关联
    val result = ctrData
      .join(cvr1Data, Seq("unitid"), "left_outer")
      .join(cvr2Data, Seq("unitid"), "left_outer")
      .withColumn("cvr1cnt", when(col("cvr1cnt").isNull, 0).otherwise(col("cvr1cnt")))
      .withColumn("cvr2cnt", when(col("cvr2cnt").isNull, 0).otherwise(col("cvr2cnt")))
//    result.write.mode("overwrite").saveAsTable("test.ocpcv3_novel_cvr_data_hourly")

    val resultDF = result.select("unitid", "new_adclass", "cvr1cnt", "cvr2cnt")

    // 返回结果
    resultDF.show(10)
    resultDF
  }

  def savePbPack(dataset: Dataset[Row]): Unit = {
    var list = new ListBuffer[SingleUnit]
    val filename = s"OcpcNovel.pb"
    println("size of the dataframe")
    println(dataset.count)
    dataset.show(10)
    dataset.printSchema()
    var cnt = 0

    for (record <- dataset.collect()) {
      val unitid = record.getAs[Int]("unitid").toString
      val cpa1History = record.getAs[Double]("cpa_history")
      val kvalue = record.getAs[Double]("kvalue")
      val cvr1cnt = record.getAs[Long]("cvr1cnt")
      val cvr2cnt = record.getAs[Long]("cvr2cnt")
      val cpa2History = 0.0
      val conversionGoal = record.getAs[Int]("conversion_goal")

      if (cnt % 100 == 0) {
        println(s"unitid:$unitid, cpa1History:$cpa1History, kvalue:$kvalue, cvr1cnt:$cvr1cnt, cvr2cnt:$cvr1cnt, cpa2History:$cpa2History, conversionGoal:$conversionGoal")
      }
      cnt += 1

      val currentItem = SingleUnit(
        unitid = unitid,
        kvalue = kvalue,
        cpaHistory = cpa1History,
        cvr2Cnt = cvr1cnt,
        cvr3Cnt = cvr2cnt,
        cpa3History = cpa2History,
        conversiongoal = conversionGoal
      )
      list += currentItem

    }
    val result = list.toArray[SingleUnit]
    val adUnitList = OcpcNovelList(
      adunit = result
    )

    println("length of the array")
    println(result.length)
    adUnitList.writeTo(new FileOutputStream(filename))

    println("complete save data into protobuffer")

  }


}
