package com.cpc.spark.ocpcV3.HotTopicOcpc.model

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import ocpc.ocpc.SingleRecord
import ocpc.ocpc.OcpcList

import scala.collection.mutable.ListBuffer

object OcpcGetPb_v2 {
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
    val version = args(2).toString

    // 暗投：不能有重复identifier
    //    dl_cpc.ocpc_pb_result_hourly
    //    dl_cpc.ocpc_prev_pb

    // 读取数据
    val base = getBaseData(date, hour, spark)
    base.write.mode("overwrite").saveAsTable("test.ocpc_hottopic_base")
    val cvrData = getCVR(date, hour, spark)
    cvrData.write.mode("overwrite").saveAsTable("test.ocpc_hottopic_cvrData")
    val cpaHistory = getCPAgiven(version, date, hour, spark)
    cpaHistory.write.mode("overwrite").saveAsTable("test.ocpc_hottopic_cpaHistory")
    val kvalue = getK( version, date, hour, spark)
    kvalue.write.mode("overwrite").saveAsTable("test.ocpc_hottopic_kvalue")
    val adclassCPA = spark
      .table("dl_cpc.ocpc_cpa_history_adclass_hourly")
      .where(s"`date`='$date' and `hour`='$hour' and `version`='$version'")
      .select("new_adclass", "cpa_adclass")
    adclassCPA.write.mode("overwrite").saveAsTable("test.ocpc_hottopic_adclassCPA")

    // 组装数据
    val data = base
      .join(cpaHistory, Seq("identifier", "new_adclass"), "left_outer")
      .select("identifier", "new_adclass", "cpa_history", "conversion_goal")
      .join(adclassCPA, Seq("new_adclass"), "left_outer")
      .withColumn("conversion_goal", when(col("cpa_history").isNull && col("conversion_goal").isNull, lit(1)).otherwise(col("conversion_goal")))
      .withColumn("cpa_given", when(col("cpa_history").isNull && col("conversion_goal") === 1, col("cpa_adclass")).otherwise(col("cpa_history")))
      .filter("cpa_given is not null and conversion_goal is not null")
    data.write.mode("overwrite").saveAsTable("test.ocpc_hottopic_data")

    val resultDF = data
      .groupBy("identifier", "conversion_goal")
      .agg(avg(col("cpa_given")).alias("cpa_given"))
      .join(cvrData, Seq("identifier", "conversion_goal"), "left_outer")
      .select("identifier", "conversion_goal", "cpa_given", "cvrcnt")
      .join(kvalue, Seq("identifier", "conversion_goal"), "left_outer")
      .select("identifier", "conversion_goal", "cpa_given", "cvrcnt", "kvalue")
      .withColumn("cvrcnt", when(col("cvrcnt").isNull, 0).otherwise(col("cvrcnt")))
      .withColumn("kvalue", when(col("kvalue").isNull, 0.0).otherwise(col("kvalue")))
      .selectExpr("cast(identifier as string) identifier", "conversion_goal", "cpa_given", "cast(cvrcnt as bigint) cvrcnt", "cast(kvalue as double) kvalue")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))

//        resultDF.write.mode("overwrite").saveAsTable("test.ocpc_hottopic_prev_pb")
      resultDF.repartition(10).write.mode("overwrite").insertInto("test.ocpc_hottopic_prev_pb_hourly")
        resultDF
          .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_pb_result_hourly")
        resultDF.write.mode("overwrite").saveAsTable("test.ocpc_hottopic_pb_result_hourly")

    savePbPack(resultDF, version)
  }

  def getBaseData(date: String, hour: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -7)
    val startdate = calendar.getTime
    val date1 = dateConverter.format(startdate)
    val selectCondition = getTimeRangeSql2(date1, hour, date, hour)

    val sqlRequest =
      s"""
         |SELECT
         |  cast(unitid as string) as identifier,
         |  adclass
         |FROM
         |  dl_cpc.ocpc_ctr_data_hourly
         |WHERE
         |  $selectCondition
         |AND
         |  media_appsid in ("80000001", "80000002", "80002819")
       """.stripMargin
    //    val resultDF = spark
    //      .table("dl_cpc.ocpcv3_ctr_data_hourly")
    //      .where(selectCondition)
    //      .withColumn("identifier", col("unitid"))
    //      .selectExpr("cast(identifier as string) identifier", "adclass")
    //      .withColumn("new_adclass", col("adclass")/1000)
    //      .withColumn("new_adclass", col("new_adclass").cast(IntegerType))
    //      .select("identifier", "new_adclass")
    //      .distinct()
    println(sqlRequest)
    val resultDF = spark
      .sql(sqlRequest)
      .withColumn("new_adclass", col("adclass")/1000)
      .withColumn("new_adclass", col("new_adclass").cast(IntegerType))
      .select("identifier", "new_adclass")
      .distinct()

    //    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_base_ctr_20181227")
    resultDF
  }

  def getCVR(date: String, hour: String, spark: SparkSession) = {
    /*
    根据ocpc_union_log_hourly关联到正在跑ocpc的广告数据
     */
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -7)
    val startdate = calendar.getTime
    val date1 = dateConverter.format(startdate)
    val selectCondition = getTimeRangeSql2(date1, hour, date, hour)

    val ocpcUnionlog = spark
      .table("dl_cpc.ocpc_union_log_hourly")
      .where(selectCondition)
      .filter(s"media_appsid = '80002819'")
      .withColumn("identifier", col("unitid"))
      .filter("isclick=1")
      .selectExpr("searchid", "cast(identifier as string) identifier")

    // cvr data


    val rawCvr1 = spark
      .table("dl_cpc.ml_cvr_feature_v1")
      .where(selectCondition)
      .filter(s"label_type!=12")
      .withColumn("iscvr1", col("label2"))
      .select("searchid", "iscvr1")
      .filter("iscvr1=1")
      .distinct()

    val rawCvr2 = spark
      .table("dl_cpc.ml_cvr_feature_v2")
      .where(selectCondition)
      .withColumn("iscvr2", col("label"))
      .select("searchid", "iscvr2")
      .filter("iscvr2=1")
      .distinct()

    // cvr1
    val cvr1Data = ocpcUnionlog
      .join(rawCvr1, Seq("searchid"), "left_outer")
      .groupBy("identifier")
      .agg(sum(col("iscvr1")).alias("cvrcnt"))
      .withColumn("conversion_goal", lit(1))
      .select("identifier", "cvrcnt", "conversion_goal")

    // cvr2
    val cvr2Data = ocpcUnionlog
      .join(rawCvr2, Seq("searchid"), "left_outer")
      .groupBy("identifier")
      .agg(sum(col("iscvr2")).alias("cvrcnt"))
      .withColumn("conversion_goal", lit(2))
      .select("identifier", "cvrcnt", "conversion_goal")

    // 数据关联
    val resultDF = cvr1Data.union(cvr2Data)
    resultDF
  }

  def getCPAgiven(version: String, date: String, hour: String, spark: SparkSession) = {
    /*
    根据cpahistory来获得cpagiven
     */
    val resultDF = spark
      .table("dl_cpc.ocpc_cpa_history_hourly")
      .where(s"`date` = '$date' and `hour` = '$hour' and version = '$version'")

    resultDF

  }

  def getK(version: String, date: String, hour: String, spark: SparkSession) = {
    /*
    pidK和regressionK外关联，优先regressionK
     */

    // pidK
    val pidK = spark
      .table("dl_cpc.ocpc_pid_k_hourly")
      .where(s"`date` = '$date' and `hour` = '$hour' and version = '$version'")
      .withColumn("pid_k", col("k_value"))
      .select("identifier", "pid_k", "conversion_goal")

    // regressionK
    val regressionK = spark
      .table("dl_cpc.ocpc_k_regression_hourly")
      .where(s"`date` = '$date' and `hour` = '$hour' and version = '$version'")
      .withColumn("regression_k", col("k_ratio"))
      .select("identifier", "regression_k", "conversion_goal")

    val resultDF = pidK
      .join(regressionK, Seq("identifier", "conversion_goal"), "outer")
      .withColumn("kvalue", when(col("regression_k").isNull, col("pid_k")).otherwise(col("regression_k")))
      .withColumn("kvalue", when(col("kvalue") < 0.0, 0.0).otherwise(when(col("kvalue")>10.0, 10.0).otherwise(col("kvalue"))))


    resultDF

  }



  def savePbPack(dataset: Dataset[Row], version: String): Unit = {
    var list = new ListBuffer[SingleRecord]
    val filename = s"Ocpc_" + version + "_unknown.pb"
    println("size of the dataframe")
    println(dataset.count)
    dataset.show(10)
    dataset.printSchema()
    var cnt = 0

    for (record <- dataset.collect()) {
      val identifier = record.getAs[String]("identifier")
      val cpaGiven = record.getAs[Double]("cpa_given")
      val kvalue = record.getAs[Double]("kvalue")
      val cvrCnt = record.getAs[Long]("cvrcnt")
      val conversionGoal = record.getAs[Int]("conversion_goal")

      if (cnt % 100 == 0) {
        println(s"identifier:$identifier, conversionGoal:$conversionGoal, cpaGiven:$cpaGiven, kvalue:$kvalue, cvrCnt:$cvrCnt")
      }
      cnt += 1

      //      string identifier = 1;
      //      int32 conversiongoal = 2;
      //      double kvalue = 3;
      //      double cpagiven = 4;
      //      int64 cvrcnt = 5;

      val currentItem = SingleRecord(
        identifier = identifier,
        conversiongoal = conversionGoal,
        kvalue = kvalue,
        cpagiven = cpaGiven,
        cvrcnt = cvrCnt
      )
      list += currentItem

    }
    val result = list.toArray[SingleRecord]
    val adRecordList = OcpcList(
      adrecord = result
    )

    println("length of the array")
    println(result.length)
    adRecordList.writeTo(new FileOutputStream(filename))

    println("complete save data into protobuffer")

  }

}
