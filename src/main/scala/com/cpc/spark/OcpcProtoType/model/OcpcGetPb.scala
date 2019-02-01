package com.cpc.spark.OcpcProtoType.model

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import ocpc.ocpc.{OcpcList, SingleRecord}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable.ListBuffer


object OcpcGetPb {
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
    // bash: 2019-01-02 12 version2 novel 1
    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val media = args(3).toString
    val isKnown = args(4).toInt
    var mediaSelection = s"media_appsid in ('80000001', '80000002')"
    if (media == "qtt") {
      mediaSelection = s"media_appsid in ('80000001', '80000002')"
    } else if (media == "novel") {
      mediaSelection = s"media_appsid in ('80001098','80001292')"
    } else {
      mediaSelection = s"media_appsid in ('80000001', '80000002', '80001098','80001292')"
    }

    // 明投：可以有重复identifier
    //    dl_cpc.ocpc_pb_result_hourly
    //    dl_cpc.ocpc_prev_pb

    // 读取数据
    val conversionGoal = 1
    val base = getBaseData(mediaSelection, date, hour, spark)
    val cvrData = getOcpcCVR(mediaSelection, conversionGoal, date, hour, spark)
    val initKdata = getInitK(mediaSelection, conversionGoal, date, hour, spark)
    //    val cpaGiven = getCPAgiven("adv", version, date, hour, spark)
    val kvalue = getK(version, date, hour, spark)

    // 组装数据
    val resultDF = assemblyPBknown(mediaSelection, base, cvrData, initKdata, kvalue, version, date, hour, spark)

    resultDF
      .repartition(10).write.mode("overwrite").insertInto("test.ocpc_prev_pb_hourly")
    //    resultDF
    // .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_pb_result_hourly")
    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_pb_result_hourly")

    savePbPack(resultDF, version, isKnown)
  }

  def getBaseData(mediaSelection: String, date: String, hour: String, spark: SparkSession) = {
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
         |  cast(unitid as string) as identifier
         |FROM
         |  dl_cpc.ocpc_ctr_data_hourly
         |WHERE
         |  $selectCondition
         |AND
         |  $mediaSelection
       """.stripMargin

    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest).distinct()

    //    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_base_ctr_20181227")
    resultDF
  }

  def getInitK(mediaSelection: String, conversionGoal: Int, date: String, hour: String, spark: SparkSession) = {
    /*
    直接获取所有的identifier的转化数据
     */
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -7)
    val startdate = calendar.getTime
    val date1 = dateConverter.format(startdate)
    val selectCondition = getTimeRangeSql2(date1, hour, date, hour)

    // ctr数据
    val sqlRequest1 =
      s"""
         |SELECT
         |  unitid,
         |  SUM(total_price) as total_cost,
         |  SUM(ctr_cnt) as ctrcnt,
         |  SUM(total_pcvr) as total_pcvr
         |FROM
         |  dl_cpc.ocpc_ctr_data_hourly
         |WHERE
         |  $selectCondition
         |AND
         |  $mediaSelection
         |GROUP BY unitid
       """.stripMargin
    println(sqlRequest1)
    val ctrData = spark
      .sql(sqlRequest1)
      .selectExpr("cast(unitid as string) identifier", "total_cost", "ctrcnt", "total_pcvr")

    // 根据conversionGoal选择cv的sql脚本
    var sqlRequest2 = ""
    if (conversionGoal == 1) {
      // cvr1数据
      sqlRequest2 =
        s"""
           |SELECT
           |  searchid,
           |  unitid,
           |  label2 as label
           |FROM
           |  dl_cpc.ml_cvr_feature_v1
           |WHERE
           |  $selectCondition
           |AND
           |  $mediaSelection
           |AND
           |  label2=1
           |AND
           |  label_type!=12
           |GROUP BY searchid, unitid, label2
       """.stripMargin
    } else {
      // cvr2数据
      sqlRequest2 =
        s"""
           |SELECT
           |  searchid,
           |  unitid,
           |  label
           |FROM
           |  dl_cpc.ml_cvr_feature_v2
           |WHERE
           |  $selectCondition
           |AND
           |  $mediaSelection
           |AND
           |  label=1
           |GROUP BY searchid, unitid, label
       """.stripMargin
    }
    println(sqlRequest2)
    val cvrData = spark
      .sql(sqlRequest2)
      .groupBy("unitid")
      .agg(sum(col("label")).alias("complete_cvrcnt"))
      .na.fill(0, Seq("cvrcnt"))
      .withColumn("conversion_goal", lit(conversionGoal))
      .selectExpr("cast(unitid as string) identifier", "cvrcnt", "conversion_goal")

    // 数据关联
    val result = ctrData
      .join(cvrData, Seq("identifier"), "left_outer")
      .select("identifier", "cvrcnt", "conversion_goal", "total_cost", "ctrcnt", "total_pcvr")
      .na.fill(0, Seq("cvrcnt"))
      .filter("cvrcnt > 0")
      .withColumn("hcvr", col("cvrcnt") * 1.0 / col("ctrcnt"))
      .withColumn("hpcvr", col("total_pcvr") * 1.0 / col("ctrcnt"))
      .withColumn("init_k", col("hcvr") * 1.0 / col("hpcvr"))

    val resultDF = result.select("identifier", "conversion_goal", "init_k")

    resultDF
  }

  def getOcpcCVR(mediaSelection: String, conversionGoal: Int, date: String, hour: String, spark: SparkSession) = {
    /*
    根据ocpc_union_log_hourly关联到正在跑ocpc的广告数据
     */
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -7)
    val startdate = calendar.getTime
    val date1 = dateConverter.format(startdate)
    val selectCondition = getTimeRangeSql2(date1, hour, date, hour)

    val ocpcUnionlog = spark
      .table("dl_cpc.ocpc_union_log_hourly")
      .where(selectCondition)
      .filter(mediaSelection)
      .withColumn("identifier", col("unitid"))
      .filter("isclick=1")
      .selectExpr("searchid", "cast(identifier as string) identifier")

    // cvr data
    // 根据conversionGoal选择cv的sql脚本
    var sqlRequest = ""
    if (conversionGoal == 1) {
      // cvr1数据
      sqlRequest =
        s"""
           |SELECT
           |  searchid,
           |  label2 as label
           |FROM
           |  dl_cpc.ml_cvr_feature_v1
           |WHERE
           |  $selectCondition
           |AND
           |  $mediaSelection
           |AND
           |  label2=1
           |AND
           |  label_type!=12
           |GROUP BY searchid, label2
       """.stripMargin
    } else {
      // cvr2数据
      sqlRequest =
        s"""
           |SELECT
           |  searchid,
           |  label
           |FROM
           |  dl_cpc.ml_cvr_feature_v2
           |WHERE
           |  $selectCondition
           |AND
           |  $mediaSelection
           |AND
           |  label=1
           |GROUP BY searchid, label
       """.stripMargin
    }
    println(sqlRequest)
    val rawCvr = spark.sql(sqlRequest)

    // 数据汇总
    val resultDF = ocpcUnionlog
      .join(rawCvr, Seq("searchid"), "left_outer")
      .groupBy("identifier")
      .agg(sum(col("label")).alias("cvrcnt"))
      .withColumn("conversion_goal", lit(conversionGoal))
      .select("identifier", "cvrcnt", "conversion_goal")

    resultDF
  }

  def getCPAgiven(src: String, version: String, date: String, hour: String, spark: SparkSession) = {
    /*
    根据cpahistory来获得cpagiven
     */
    // 根据数据源选择sql脚本
    var sqlRequest = ""
    if (src == "history") {
      sqlRequest =
        s"""
           |SELECT
           |  *
           |FROM
           |  dl_cpc.ocpc_cpa_history_hourly
           |WHERE
           |  `date` = '$date' and `hour` = '$hour' and version = '$version'
         """.stripMargin
    } else {
      sqlRequest =
        s"""
           |SELECT
           |    t.unitid,
           |    cast(t.unitid as string) as identifier,
           |    t.cpa_given,
           |    t.conversion_goal
           |FROM
           |    (SELECT
           |        unitid,
           |        cpa_given,
           |        conversion_goal,
           |        update_timestamp,
           |        row_number() over(partition by unitid order by update_timestamp desc) as seq
           |    FROM
           |        dl_cpc.ocpc_cpa_given_hourly
           |    WHERE
           |        `date`='$date'
           |    AND
           |        `hour`='$hour') as t
           |WHERE
           |    t.seq=1
         """.stripMargin
    }
    println(sqlRequest)
    val resultDF = spark
      .sql(sqlRequest)
      .select("identifier", "cpa_given", "conversion_goal")

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
      .withColumn("kvalue", when(col("kvalue") < 0.0, 0.0).otherwise(when(col("kvalue")>2.0, 2.0).otherwise(col("kvalue"))))


    resultDF

  }

  def getOcpcFlag(mediaSelection: String, date: String, hour: String, spark: SparkSession) = {
    /*
    根据历史记录判断某个identifer是否有ocpc记录：
    如果有，ocpc_flag=1
    否则，ocpc_flag=0
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

    val sqlRequest =
      s"""
         |SELECT
         |  cast(unitid as string) as identifier
         |FROM
         |  dl_cpc.ocpc_union_log_hourly
         |WHERE
         |  $selectCondition
         |AND
         |  $mediaSelection
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark
      .sql(sqlRequest)
      .distinct()
      .withColumn("ocpc_flag", lit(1))
      .select("identifier", "ocpc_flag")

    resultDF
  }

  //  base, cvrData, cvrCompleteData, cpaGiven, kvalue, version, date, hour, spark
  def assemblyPBknown(mediaSelection: String, base: DataFrame, cvrData: DataFrame, initK: DataFrame, kvalue: DataFrame, version: String, date: String, hour: String, spark: SparkSession) = {
    val ocpc_flag = getOcpcFlag(mediaSelection, date, hour, spark)

    val data = base
      .join(cvrData, Seq("identifier"), "left_outer")
      .withColumn("cpa_given", lit(1))
      .select("identifier", "conversion_goal", "cpa_given", "cvrcnt")
      .join(ocpc_flag, Seq("identifier"), "left_outer")
      .select("identifier", "conversion_goal", "cpa_given", "cvrcnt", "ocpc_flag")
      .join(initK, Seq("identifier", "conversion_goal"), "left_outer")
      .select("identifier", "conversion_goal", "cpa_given", "cvrcnt", "ocpc_flag", "init_k")
      .join(kvalue, Seq("identifier", "conversion_goal"), "left_outer")
      .select("identifier", "conversion_goal", "cpa_given", "cvrcnt", "ocpc_flag", "init_k", "kvalue")
      .withColumn("cvrcnt", when(col("cvrcnt").isNull, 0).otherwise(col("cvrcnt")))

    val resultDF = data
      .withColumn("kvalue", when(col("ocpc_flag")===1, col("init_k")).otherwise(col("kvalue")))
      .selectExpr("cast(identifier as string) identifier", "conversion_goal", "cpa_given", "cast(cvrcnt as bigint) cvrcnt", "cast(kvalue as double) kvalue")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))

    resultDF
  }



  def savePbPack(dataset: Dataset[Row], version: String, isKnown: Int): Unit = {
    var list = new ListBuffer[SingleRecord]
    var filename = ""
    if (isKnown == 1) {
      filename = s"Ocpc_" + version + "_known.pb"
    } else {
      filename = s"Ocpc_" + version + "_unknown.pb"
    }
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


