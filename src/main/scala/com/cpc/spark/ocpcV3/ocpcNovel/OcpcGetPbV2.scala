package com.cpc.spark.ocpcV3.ocpcNovel

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import ocpcnovel.ocpcnovel.SingleUnit
import ocpcnovel.ocpcnovel.OcpcNovelList
import org.apache.spark.sql.types.{DoubleType, IntegerType}

import scala.collection.mutable.ListBuffer


object OcpcGetPbV2 {
  def main(args: Array[String]): Unit = {
    /*
    组装pb文件，由以下几个部分构成：
    - unitid：标识符，广告单元
    - cpahistory：历史cpa
    - cvr1cnt和cvr2cnt：前72小时的转化数，转化数，决定是否进入第二阶段，同时作为主表
    - kvalue：反馈系数，对cvr模型的系统偏差校准
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    // 读取数据
    val base = getCVR(date, hour, spark)
    val cpaHistory = getCPAhistory(date, hour, spark)
    val kvalue = getK(base, cpaHistory, date, hour, spark)
    val adclassCPA = spark
      .table("dl_cpc.ocpcv3_cpa_history_v2_adclass_hourly")
      .where(s"`date`='$date' and `hour`='$hour'")
      .select("new_adclass", "cpa1", "cpa2")

    // 组装数据
    val data = base
      .join(cpaHistory, Seq("unitid", "new_adclass"), "left_outer")
      .select("unitid", "new_adclass", "cpa_history", "cvr1cnt", "cvr2cnt")
      .join(kvalue, Seq("unitid", "new_adclass"), "left_outer")
      .select("unitid", "new_adclass", "cpa_history", "cvr1cnt", "cvr2cnt", "kvalue", "conversion_goal")
      .join(adclassCPA, Seq("new_adclass"), "left_outer")
      .select("unitid", "new_adclass", "cpa_history", "cvr1cnt", "cvr2cnt", "kvalue", "conversion_goal", "cpa1", "cpa2")
      .withColumn("adclass_cpa", when(col("conversion_goal")===1, col("cpa1")).otherwise(col("cpa2")))
      .withColumn("cpa_history", when(col("cpa_history").isNull, col("adclass_cpa")).otherwise(col("cpa_history")))
      .withColumn("cpa_history", when(col("cpa_history") > 50000, 50000).otherwise(col("cpa_history")))
      .withColumn("kvalue", when(col("kvalue").isNull, 0.0).otherwise(col("kvalue")))
        .withColumn("date", lit(date))
        .withColumn("hour", lit(hour))

//    data.write.mode("overwrite").saveAsTable("test.ocpcv3_novel_pb_v2_hourly_middle")
    data
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpcv3_novel_pb_v2_hourly_middle")

    val resultDF = data
      .filter(s"kvalue >= 0 and cpa_history > 0 and cvr1cnt >= 0 and cvr2cnt >= 0 and conversion_goal>0")
      .groupBy("unitid")
      .agg(
        avg(col("kvalue")).alias("kvalue"),
        avg(col("cpa_history")).alias("cpa_history"),
        sum(col("cvr1cnt")).alias("cvr1cnt"),
        sum(col("cvr2cnt")).alias("cvr2cnt"),
        avg(col("conversion_goal")).alias("conversion_goal"))
      .withColumn("conversion_goal", when(col("conversion_goal")===2, 2).otherwise(1))
      .select("unitid", "cpa_history", "kvalue", "cvr1cnt", "cvr2cnt", "conversion_goal")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    val tableName = "dl_cpc.ocpcv3_novel_pb_v2_hourly"
    resultDF.write.mode("overwrite").saveAsTable("dl_cpc.ocpcv3_novel_pb_v2_once")
    resultDF
      .repartition(10).write.mode("overwrite").insertInto(tableName)
//    resultDF.write.mode("overwrite").saveAsTable("test.ocpcv3_check_novel_pb")

    savePbPack(resultDF)

  }

  def getCVR(date: String, hour: String, spark: SparkSession) = {
    /*
    7天的转化数据，分行业类别
     */
    // 计算日期周期
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val end_date = sdf.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(end_date)
    calendar.add(Calendar.DATE, -7)
    val start_date = calendar.getTime
    val date1 = sdf.format(start_date)
    val selectCondition = getTimeRangeSql2(date1, hour, date, hour)
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
//    ctrData.write.mode("overwrite").saveAsTable("test.ocpcv3_novel_ctr_data_hourly_v2")

    // cvr data
    // cvr1 or cvr3 data
    val sqlRequestCvr1Data =
    s"""
       |SELECT
       |  unitid,
       |  adclass,
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
      .withColumn("new_adclass", col("adclass")/1000)
      .withColumn("new_adclass", col("new_adclass").cast(IntegerType))
      .groupBy("unitid", "new_adclass")
      .agg(sum(col("cvr1_cnt")).alias("cvr1cnt"))
    cvr1Data.show(10)

    // cvr2data
    val sqlRequestCvr2Data =
      s"""
         |SELECT
         |  unitid,
         |  adclass,
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
      .withColumn("new_adclass", col("adclass")/1000)
      .withColumn("new_adclass", col("new_adclass").cast(IntegerType))
      .groupBy("unitid", "new_adclass")
      .agg(sum(col("cvr2_cnt")).alias("cvr2cnt"))
    cvr2Data.show(10)

    // 数据关联
    val result = ctrData
      .join(cvr1Data, Seq("unitid", "new_adclass"), "left_outer")
      .join(cvr2Data, Seq("unitid", "new_adclass"), "left_outer")
      .withColumn("cvr1cnt", when(col("cvr1cnt").isNull, 0).otherwise(col("cvr1cnt")))
      .withColumn("cvr2cnt", when(col("cvr2cnt").isNull, 0).otherwise(col("cvr2cnt")))

    val resultDF = result.select("unitid", "new_adclass", "cvr1cnt", "cvr2cnt")
//    resultDF.write.mode("overwrite").saveAsTable("test.ocpcv3_novel_cvr_data_hourly_v2")

    // 返回结果
    resultDF.show(10)
    resultDF
  }

  def getCPAhistory(date: String, hour: String, spark: SparkSession) = {
    /*
    抽取v2版本的cpa history
     */
    val tableName = "dl_cpc.ocpcv3_novel_cpa_history_hourly_v2"
    val resultDF = spark
      .table(tableName)
      .where(s"`date`='$date' and `hour`='$hour'")
    resultDF.show(10)
    resultDF
  }

  def getK(base: DataFrame, cpaHistory: DataFrame, date: String, hour: String, spark: SparkSession) = {
    /*
    1. 先从基础表抽取所有unitid，再从cpahistory抽取conversion goal，重新分配conversiongoal
    2. 按照conversion goal分配kvalue
     */
    // 确定主表的conversion goal
    val rawData = base
      .join(cpaHistory, Seq("unitid", "new_adclass"), "left_outer")
      .select("unitid", "new_adclass", "conversion_goal")
      .withColumn("conversion_goal", when(col("conversion_goal").isNull, 1).otherwise(col("conversion_goal")))

    // 读取k
    val tableName1 = "dl_cpc.ocpc_v3_novel_k_regression_v2"
    val rawData1 = spark
      .table(tableName1)
      .where(s"`date`='$date' and `hour`='$hour'")
      .select("unitid", "k_ratio1", "k_ratio2")
    rawData1.show(10)


    val tableName2 = "dl_cpc.ocpc_novel_k_value_table_v2"
    val rawData2 = spark
      .table(tableName2)
      .where(s"`date`='$date' and `hour`='$hour'")
      .groupBy("unitid", "new_adclass")
      .agg(avg(col("k_value")).alias("k_value"))
      .select("unitid", "new_adclass", "k_value")
    rawData2.show(10)

    val kvalues = rawData1
      .join(rawData2, Seq("unitid"), "outer")
      .select("unitid", "new_adclass", "k_value", "k_ratio1", "k_ratio2")
      .filter("new_adclass is not null")

    // 关联主表，根据conversion goal选择k
    val data = rawData
      .join(kvalues, Seq("unitid", "new_adclass"), "left_outer")
      .select("unitid", "new_adclass", "k_value", "conversion_goal", "k_ratio1", "k_ratio2")
      .filter("new_adclass is not null and conversion_goal is not null")
      .withColumn("k_ratio", when(col("conversion_goal") === 2, col("k_ratio2")).otherwise(col("k_ratio1")))
      .withColumn("kvalue", when(col("k_ratio").isNull, col("k_value")).otherwise(col("k_ratio")))
      .filter(s"kvalue > 0 or kvalue is null")
      .withColumn("kvalue", when(col("kvalue") > 15.0, 15.0).otherwise(col("kvalue")))
      .withColumn("kvalue", when(col("kvalue") < 0.1, 0.1).otherwise(col("kvalue")))

    val resultDF = data.select("unitid", "new_adclass", "kvalue", "conversion_goal")

    // TODO 删除临时表
//    data.write.mode("overwrite").saveAsTable("test.ocpcv3_novel_kvalue_data_hourly_v2")

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