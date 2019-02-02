package com.cpc.spark.OcpcProtoType.model

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.{getTimeRangeSql2, getTimeRangeSql3}
import ocpc.ocpc.{OcpcList, SingleRecord}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable.ListBuffer


object OcpcGetPb {
  def main(args: Array[String]): Unit = {
    /*
    pb文件格式：
    string identifier = 1;
    int32 conversiongoal = 2;
    double kvalue = 3;
    double cpagiven = 4;
    int64 cvrcnt = 5;
    对于明投广告，cpagiven=1， cvrcnt使用ocpc广告记录进行关联，k需要进行计算，每个conversiongoal都需要进行计算


     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    // bash: 2019-01-02 12 qtt_demo qtt 1
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
      mediaSelection = s"media_appsid = '80002819'"
    }

//    // 明投：可以有重复identifier
//    dl_cpc.ocpc_pb_result_hourly_v2
//    dl_cpc.ocpc_prev_pb_once
    val kvalue = getPbByConversion(mediaSelection, 1, version, date, hour, spark)
    kvalue.write.mode("overwrite").saveAsTable("test.check_ocpc_data20190202")
//
//    // 组装数据
//    val resultDF = assemblyPBknown(mediaSelection, base, cvrData, initKdata, kvalue, version, date, hour, spark)
//    val cvrData = getOcpcCVR(mediaSelection, conversionGoal, date, hour, spark)
//    val initKdata = getInitK(mediaSelection, conversionGoal, date, hour, spark)
//    //    val cpaGiven = getCPAgiven("adv", version, date, hour, spark)
//    val kvalue = getK(version, date, hour, spark)
//
//    // 组装数据
//    val resultDF = assemblyPBknown(mediaSelection, base, cvrData, initKdata, kvalue, version, date, hour, spark)
//
//    resultDF
//      .repartition(10).write.mode("overwrite").insertInto("test.ocpc_prev_pb_hourly")
//    //    resultDF
//    // .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_pb_result_hourly")
//    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_pb_result_hourly")
//
//    savePbPack(resultDF, version, isKnown)
  }

  def getPbByConversion(mediaSelection: String, conversionGoal: Int, version: String, date: String, hour: String, spark: SparkSession) = {
    /*
    计算步骤
    1. 获取base_data
    2. 按照conversiongoal, 计算cvrcnt，数据串联
    3. 计算k
     */
    val base = getBaseData(mediaSelection, conversionGoal, date, hour, spark)
    val cvrData = getOcpcCVR(mediaSelection, conversionGoal, date, hour, spark)
    val kvalue = getKvalue(mediaSelection, conversionGoal, version, date, hour, spark)

    val resultDF = base
      .join(cvrData, Seq("identifier", "conversion_goal"), "left_outer")
      .join(kvalue, Seq("identifier", "conversion_goal"), "left_outer")
      .select("identifier", "conversion_goal", "cvrcnt", "kvalue")


    resultDF
  }

  def getKvalue(mediaSelection: String, conversionGoal: Int, version: String, date: String, hour: String, spark: SparkSession) = {
    /*
    4个来源:
      1. regression计算结果
      2. pid计算结果
      3. 上一次pb文件
      4. 最近三天pcoc

    需要分情况选择k值:有两个标签isocpc和history_ocpc_flag
    前者表示生成pb文件时是否开启ocpc，后者表示最近7天是否有ocpc广告投放记录
    分别用a,b表示，计算顺序为下
      1.  a为1：ocpc投放的k值
      按照以下来源次序计算: 1 2 3, 需要控制增长速率，另外如果一段时间内ocpc广告没有投放记录，不增加k值
      2.  b为0：cpc投放的k值
      根据pcoc计算的值
      3.  两组数据外关联，b为0的情况优先级更高，获得最终pb文件
     */

    // ocpc投放的k值
    val regressionK = getRegressionK(conversionGoal, version, date, hour, spark)
    val pidK = getPidK(conversionGoal, version, date, hour, spark)
    val prevPb = getPrevPb(conversionGoal, version, date, hour, spark)
    val ocpcK = calculateKocpc(regressionK, pidK, prevPb, spark)

    // cpc投放的k值
    val cpcK = getCpcK(mediaSelection, conversionGoal, date, hour, spark)

    // 数据外关联
    val ocpcKfinal = ocpcK
        .withColumn("ocpc_k", col("k_value"))
        .select("identifier", "ocpc_k")
    val cpcKfinal = cpcK
        .withColumn("cpc_k", col("kvalue"))
        .select("identifier", "cpc_k", "history_ocpc_flag")

    val finalK = ocpcKfinal
      .join(cpcKfinal, Seq("identifier"), "outer")
      .select("identifier", "ocpc_k", "cpc_k", "history_ocpc_flag")
      .na.fill(0, Seq("ocpc_k", "cpc_k", "history_ocpc_flag"))
      .withColumn("kvalue", when(col("history_ocpc_flag") === 0, col("ocpc_k")).otherwise(col("cpc_k")))
      .withColumn("conversion_goal", lit(conversionGoal))

    val resultDF = finalK.select("identifier", "kvalue", "conversion_goal")

    resultDF

  }

  def getCpcK(mediaSelection: String, conversionGoal: Int, date: String, hour: String, spark: SparkSession) = {
    /*
     通过slim_union_log关联的方式获取前72小时中的k值
     1. 以searchid关联的方式关联k值与cvr
     2. 计算各个identifier的实际cvr
     3. 按照实际cvr的2倍过滤过高cvr
      */
    // 对于刚进入ocpc阶段但是有cpc历史数据的广告依据历史转化率给出k的初值
    // cvr 分区
    var cvrGoal = ""
    if (conversionGoal == 1) {
      cvrGoal = "cvr1"
    } else if (conversionGoal == 2) {
      cvrGoal = "cvr2"
    } else {
      cvrGoal = "cvr3"
    }

    // 取历史数据
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val end_date = sdf.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(end_date)
    calendar.add(Calendar.DATE, -3)
    val dt = calendar.getTime
    val date1 = sdf.format(dt)
    val selectCondition = getTimeRangeSql3(date1, hour, date, hour)

    calendar.add(Calendar.DATE, -4)
    val dt2 = calendar.getTime
    val date2 = sdf.format(dt2)
    val selectCondition2 = getTimeRangeSql2(date2, hour, date, hour)

    // history_ocpc_flag标签
    val sqlRequest1 =
      s"""
         |SELECT
         |  searchid,
         |  cast(unitid as string) as identifier,
         |  1 as history_ocpc_flag
         |FROM
         |  dl_cpc.ocpc_filter_unionlog
         |WHERE
         |  $selectCondition2
         |AND
         |  $mediaSelection
         |AND
         |  is_ocpc = 1
       """.stripMargin
    println(sqlRequest1)
    val ocpcHistoryData = spark
      .sql(sqlRequest1)
      .select("identifier", "history_ocpc_flag")
      .distinct()

    // 取数
    val sqlRequest2 =
      s"""
         |SELECT
         |    a.searchid,
         |    cast(a.unitid as string) identifier,
         |    a.exp_cvr,
         |    a.isclick,
         |    b.iscvr
         |FROM
         |    (SELECT
         |        searchid,
         |        unitid,
         |        exp_cvr * 1.0 / 1000000 as exp_cvr,
         |        isclick
         |    FROM
         |        dl_cpc.slim_union_log
         |    WHERE
         |        $selectCondition
         |    AND
         |        isclick=1
         |    AND
         |        media_appsid  in ('80000001', '80000002')
         |    AND antispam = 0
         |    AND ideaid > 0
         |    AND adsrc = 1
         |    AND adslot_type in (1,2,3)) as a
         |LEFT JOIN
         |    (SELECT
         |        searchid,
         |        label as iscvr
         |    FROM
         |        dl_cpc.ocpc_label_cvr_hourly
         |    WHERE
         |        `date`>='$date1'
         |    AND
         |        cvr_goal = '$cvrGoal') as b
         |ON
         |    a.searchid=b.searchid
       """.stripMargin
    println(sqlRequest2)
    val data = spark.sql(sqlRequest2)
    val cvrData = data
      .na.fill(0, Seq("iscvr"))
      .groupBy("identifier")
      .agg(
        sum(col("isclick")).alias("click"),
        sum(col("iscvr")).alias("conversion")
      )
      .withColumn("post_cvr", col("conversion") * 1.0 / col("click"))
      .withColumn("post_cvr_cali", col("post_cvr") * 5.0)
      .select("identifier", "post_cvr", "post_cvr_cali")

    val caliData = data
      .join(cvrData, Seq("identifier"), "left_outer")
      .select("searchid", "identifier", "exp_cvr", "isclick", "iscvr", "post_cvr", "post_cvr_cali")
      .withColumn("pre_cvr", when(col("exp_cvr")> col("post_cvr_cali"), col("post_cvr_cali")).otherwise(col("exp_cvr")))
      .select("searchid", "identifier", "exp_cvr", "isclick", "iscvr", "post_cvr", "pre_cvr", "post_cvr_cali")

    val resultDF = caliData
      .groupBy("identifier")
      .agg(
        sum(col("pre_cvr")).alias("pre_cvr"),
        sum(col("isclick")).alias("click"),
        sum(col("iscvr")).alias("conversion")
      )
      .withColumn("pre_cvr", col("pre_cvr") * 1.0 / col("click"))
      .select("identifier", "pre_cvr", "click", "conversion")
      .join(cvrData, Seq("identifier"), "left_outer")
      .withColumn("kvalue", col("post_cvr") * 1.0 / col("pre_cvr"))
      .select("identifier", "kvalue", "pre_cvr", "post_cvr", "click", "conversion")
      .join(ocpcHistoryData, Seq("identifier"), "left_outer")
      .select("identifier", "kvalue", "pre_cvr", "post_cvr", "click", "conversion", "history_ocpc_flag")
      .na.fill(0, Seq("history_ocpc_flag"))

    resultDF
  }


  def calculateKocpc(regressionK: DataFrame, pidK: DataFrame, prevPb: DataFrame, spark: SparkSession) = {
    val resultDF = pidK
      .join(regressionK, Seq("identifier"), "outer")
      .select("identifier", "regression_k", "pid_k")
      .withColumn("new_k", when(col("regression_k").isNotNull && col("regression_k") > 0, col("regression_k")).otherwise(col("pid_k")))
      .join(prevPb, Seq("identifier"), "outer")
      .select("identifier", "regression_k", "pid_k", "new_k", "prev_k", "flag")
      .withColumn("kvalue_middle", when(col("new_k").isNotNull && col("prev_k").isNotNull && col("new_k") > col("prev_k"), col("prev_k") + (col("new_k") - col("prev_k")) * 1.0 / 4.0).otherwise(col("new_k")))
      .withColumn("k_value", when(col("flag") === 0, col("prev_k")).otherwise(col("kvalue_middle")))
      .select("identifier", "regression_k", "pid_k", "new_k", "prev_k", "flag", "kvalue_middle", "k_value")

    resultDF
  }

  def getPrevPb(conversionGoal: Int, version: String, date: String, hour: String, spark: SparkSession) = {
    var prevTable = getPrevK(date, hour, 1, spark)
    var hourCnt=1
    while (hourCnt < 11) {
      val cnt = prevTable.count()
      println(s"check prevTable Count: $cnt, at hourCnt = $hourCnt")
      if (cnt>0) {
        hourCnt = 11
      } else {
        hourCnt += 1
        prevTable = getPrevK(date, hour, hourCnt, spark)
      }

    }

    val resultDF = prevTable
      .filter(s"conversion_goal=$conversionGoal")
      .select("identifier", "prev_k", "flag")

    resultDF
  }

  def getPrevK(date: String, hour: String, hourCnt: Int, spark: SparkSession) = {
    /*
    1.  获取上一次的pb文件中的k值
    2.  获取从上一次生成pb文件到现在的时间段之间每个identifier的点击数，根据点击数是否为0来设置flag
     */
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -hourCnt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition1 = s"`date`='$date1' and `hour`='$hour1'"
    val selectCondition2 = getTimeRangeSql2(date1, hour1, date, hour)

    // 获取上一次的pb文件中的k值
    val sqlRequest1 =
      s"""
         |SELECT
         |  identifier,
         |  conversion_goal,
         |  kvalue as prev_k
         |FROM
         |  dl_cpc.ocpc_pb_result_hourly_v2
         |WHERE
         |  $selectCondition1
       """.stripMargin
    println(sqlRequest1)
    val prevK = spark.sql(sqlRequest1)

    // 获取从上一次生成pb文件到现在的时间段之间每个identifier的点击数，根据点击数是否为0来设置flag
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  cast(unitid as string) as identifier,
         |  isclick
         |FROM
         |  dl_cpc.ocpc_filter_unionlog
         |WHERE
         |  $selectCondition2
       """.stripMargin
    println(sqlRequest2)
    val prevCtr = spark
      .sql(sqlRequest2)
      .groupBy("identifier")
      .agg(
        sum(col("isclick")).alias("ctrcnt")
      )
      .select("identifier", "ctrcnt")


    val prevTable = prevK
      .join(prevCtr, Seq("identifier"), "left_outer")
      .select("identifier", "conversion_goal", "prev_k", "ctrcnt")
      .na.fill(0, Seq("ctrcnt"))
      .withColumn("flag", when(col("ctrcnt")>0, 1).otherwise(0))
      .withColumn("date", lit(date1))
      .withColumn("hour", lit(hour1))

    prevTable
  }


  def getPidK(conversionGoal: Int, version: String, date: String, hour: String, spark: SparkSession) = {
    val selectCondition = s"`date`='$date' and `hour`='$hour' and version='$version' and conversion_goal=$conversionGoal"
    val sqlRequest =
      s"""
         |SELECT
         |  identifier,
         |  k_value as pid_k
         |FROM
         |  dl_cpc.ocpc_pid_k_hourly
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)
    resultDF
  }

  def getRegressionK(conversionGoal: Int, version: String, date: String, hour: String, spark: SparkSession) = {
    val selectCondition = s"`date`='$date' and `hour`='$hour' and version='$version' and conversion_goal=$conversionGoal"
    val sqlRequest =
      s"""
         |SELECT
         |  identifier,
         |  k_ratio as regression_k
         |FROM
         |  dl_cpc.ocpc_k_regression_hourly
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)
    resultDF
  }

  def getBaseData(mediaSelection: String, conversionGoal: Int, date: String, hour: String, spark: SparkSession) = {
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
    val resultDF = spark
      .sql(sqlRequest)
      .withColumn("conversion_goal", lit(conversionGoal))
      .distinct()

    resultDF
  }


  def getOcpcCVR(mediaSelection: String, conversionGoal: Int, date: String, hour: String, spark: SparkSession) = {
    /*
    根据ocpc_union_log_hourly关联到正在跑ocpc的广告数据
     */
    // cvr 分区
    var cvrGoal = ""
    if (conversionGoal == 1) {
      cvrGoal = "cvr1"
    } else if (conversionGoal == 2) {
      cvrGoal = "cvr2"
    } else {
      cvrGoal = "cvr3"
    }

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
    // 抽取数据
    val sqlRequest =
    s"""
       |SELECT
       |  searchid,
       |  label
       |FROM
       |  dl_cpc.ocpc_label_cvr_hourly
       |WHERE
       |  ($selectCondition)
       |AND
       |  (cvr_goal = '$cvrGoal')
       """.stripMargin
    println(sqlRequest)
    val rawCvr = spark.sql(sqlRequest)

    // 数据汇总
    val resultDF = ocpcUnionlog
      .join(rawCvr, Seq("searchid"), "left_outer")
      .groupBy("identifier")
      .agg(sum(col("label")).alias("cvrcnt"))
      .withColumn("conversion_goal", lit(conversionGoal))
      .na.fill(0, Seq("cvrcnt"))
      .select("identifier", "cvrcnt", "conversion_goal")

    resultDF
  }

//  def getCPAgiven(src: String, version: String, date: String, hour: String, spark: SparkSession) = {
//    /*
//    根据cpahistory来获得cpagiven
//     */
//    // 根据数据源选择sql脚本
//    var sqlRequest = ""
//    if (src == "history") {
//      sqlRequest =
//        s"""
//           |SELECT
//           |  *
//           |FROM
//           |  dl_cpc.ocpc_cpa_history_hourly
//           |WHERE
//           |  `date` = '$date' and `hour` = '$hour' and version = '$version'
//         """.stripMargin
//    } else {
//      sqlRequest =
//        s"""
//           |SELECT
//           |    t.unitid,
//           |    cast(t.unitid as string) as identifier,
//           |    t.cpa_given,
//           |    t.conversion_goal
//           |FROM
//           |    (SELECT
//           |        unitid,
//           |        cpa_given,
//           |        conversion_goal,
//           |        update_timestamp,
//           |        row_number() over(partition by unitid order by update_timestamp desc) as seq
//           |    FROM
//           |        dl_cpc.ocpc_cpa_given_hourly
//           |    WHERE
//           |        `date`='$date'
//           |    AND
//           |        `hour`='$hour') as t
//           |WHERE
//           |    t.seq=1
//         """.stripMargin
//    }
//    println(sqlRequest)
//    val resultDF = spark
//      .sql(sqlRequest)
//      .select("identifier", "cpa_given", "conversion_goal")
//
//    resultDF
//
//  }

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
      .withColumn("kvalue", when(col("kvalue") < 0.0, 0.0).otherwise(col("kvalue")))


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


