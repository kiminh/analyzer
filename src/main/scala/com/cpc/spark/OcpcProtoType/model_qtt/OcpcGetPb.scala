package com.cpc.spark.OcpcProtoType.model_qtt

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.{getTimeRangeSql2, getTimeRangeSql3}
import ocpc.ocpc.{OcpcList, SingleRecord}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import com.cpc.spark.udfs.Udfs_wj._
import com.typesafe.config.ConfigFactory

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
    // bash: 2019-01-02 12 1 qtt_demo qtt
    val date = args(0).toString
    val hour = args(1).toString
    val conversionGoal = args(2).toInt
    val version = args(3).toString
    val media = args(4).toString

    println("parameters:")
    println(s"date=$date, hour=$hour, conversionGoal=$conversionGoal, version=$version, media=$media")
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
    val result = getPbByConversion(mediaSelection, conversionGoal, version, date, hour, spark)
    val resultDF = result
        .withColumn("cpagiven", lit(1))
        .select("identifier", "cpagiven", "cvrcnt", "kvalue")
        .withColumn("conversion_goal", lit(conversionGoal))
        .withColumn("date", lit(date))
        .withColumn("hour", lit(hour))
        .withColumn("version", lit(version))

    resultDF
      .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_pb_result_hourly_20190303")
//      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_pb_result_hourly_v2")

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
    val kvalue1 = getKvalue(mediaSelection, conversionGoal, version, date, hour, spark)
    val kvalue2 = smoothKvalue(kvalue1, mediaSelection, conversionGoal, version, date, hour, spark)
    val kvalue = setKvalueByUnitid(kvalue2, mediaSelection, conversionGoal, version, date, hour, spark)

    val resultDF = base
      .join(cvrData, Seq("identifier", "conversion_goal"), "left_outer")
      .join(kvalue, Seq("identifier", "conversion_goal"), "left_outer")
      .select("identifier", "conversion_goal", "cvrcnt", "kvalue")
      .na.fill(0, Seq("cvrcnt", "kvalue"))
      .withColumn("kvalue", when(col("kvalue") > 15.0, 15.0).otherwise(col("kvalue")))


    resultDF
  }

  def setKvalueByUnitid(kvalue: DataFrame, mediaSelection: String, conversionGoal: Int, version: String, date: String, hour: String, spark: SparkSession) = {
    // set the unitid that we need to reset
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "ocpc_all.ocpc_reset_k"
    val expDataPath = conf.getString(conf_key)
    val rawData = spark.read.format("json").json(expDataPath)
    val data = rawData
      .filter(s"kvalue > 0")
      .select("identifier", "conversion_goal", "kvalue")
      .groupBy("identifier", "conversion_goal")
      .agg(avg(col("kvalue")).alias("kvalue_bak"))
      .select("identifier", "conversion_goal", "kvalue_bak")
    data.show(10)

    val result = kvalue
      .withColumn("kvalue_ori", col("kvalue"))
      .join(data, Seq("identifier", "conversion_goal"), "left_outer")
      .select("identifier", "kvalue_ori", "conversion_goal", "kvalue_bak")
      .withColumn("kvalue", when(col("kvalue_bak").isNotNull, col("kvalue_bak")).otherwise(col("kvalue_ori")))
      .filter(s"kvalue is not null")

//    result.write.mode("overwrite").saveAsTable("test.set_kvalue_by_unitid20190318")

    val resultDF = result
      .select("identifier", "kvalue", "conversion_goal")

    resultDF
  }

  def udfHourDiffToFactor() = udf((hours: Int) => {
    /*
    根据最近两天里面有点击的ocpc投放小时数来计算限制阈值：
    factor = hours / 12
    如果hours >= 6, factor = 1
    factor的作用：限制k值区间
    (1 - factor) * basek <= k <= (1 + factor) * basek
     */
    var result = 0.0
    if (hours >= 12) {
      result = 0.0
    } else {
      result = hours / 12.0
    }
    result
  })

  def smoothKvalue(kvalue: DataFrame, mediaSelection: String, conversionGoal: Int, version: String, date: String, hour: String, spark: SparkSession) = {
    /*
    计算在投ocpc广告每个广告最近两天的ocpc投放小时数，并与ocpc_suggest_cpa_k_once数据表内关联，
    1. 投放小时数少于24小时且ocpc_suggest_cpa_k_once数据表有数据则按照小时数限制k值变动
    2. 投放小时数大于24小时或ocpc_k_smooth_v1数据表没有关联到数据，则不做限制
     */
    // 抽取ocpc_suggest_cpa_k_once表
    // todo
    val baseK = spark
        .table("dl_cpc.ocpc_suggest_cpa_k_once")
//        .table("dl_cpc.ocpc_suggest_cpa_k")
        .where(s"version = '$version' and conversion_goal = $conversionGoal and duration <= 3")
        .withColumn("base_k", col("kvalue"))
        .select("identifier", "base_k")

    baseK.show(10)

    // 抽取所有投放ocpc的广告单元当前有点击的ocpc投放小时数
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -2)
    val yesterday1 = calendar.getTime
    val date1 = dateConverter.format(yesterday1)
    val selectCondition = getTimeRangeSql2(date1, hour, date, hour)

    val sqlRequest =
      s"""
         |SELECT
         |    searchid,
         |    unitid,
         |    userid,
         |    isclick,
         |    date,
         |    hour
         |FROM
         |    dl_cpc.ocpc_filter_unionlog
         |WHERE
         |    $selectCondition
         |and is_ocpc=1
         |and $mediaSelection
         |and round(adclass/1000) != 132101  --去掉互动导流
         |and isclick = 1
         |and ideaid > 0
         |and adsrc = 1
         |and adslot_type in (1,2,3)
         |and searchid is not null
         |and cast(ocpc_log_dict['conversiongoal'] as int) = $conversionGoal
       """.stripMargin
    println(sqlRequest)
    val ocpcRecord = spark
      .sql(sqlRequest)
      .groupBy("unitid", "date", "hour")
      .agg(sum(col("isclick")).alias("click"))
      .withColumn("identifier", col("unitid"))
      .select("identifier", "click", "date", "hour")
      .filter(s"click>0")

    ocpcRecord.show(10)
    ocpcRecord.createOrReplaceTempView("ocpc_record")

    val sqlRequest2 =
      s"""
         |SELECT
         |  identifier,
         |  count(1) as hour_cnt
         |FROM
         |  ocpc_record
         |GROUP BY identifier
       """.stripMargin
    println(sqlRequest2)
    val ocpcRecordCnt = spark.sql(sqlRequest2).filter(s"hour_cnt < 12")

    // 数据关联
    val joinData = ocpcRecordCnt
      .join(baseK, Seq("identifier"), "inner")
      .select("identifier", "base_k", "hour_cnt")
      .withColumn("factor", udfHourDiffToFactor()(col("hour_cnt")))

    joinData.createOrReplaceTempView("join_data")
    val sqlRequest3 =
      s"""
         |SELECT
         |  identifier,
         |  base_k,
         |  hour_cnt,
         |  factor,
         |  (1 - factor) * base_k as bottom_k,
         |  (1 + factor) * base_k as top_k,
         |  1 as flag
         |FROM
         |  join_data
       """.stripMargin
    println(sqlRequest3)
    val kRegion = spark.sql(sqlRequest3)

    // 重新计算k值
    val result = kvalue
      .withColumn("original_k", col("kvalue"))
      .join(kRegion, Seq("identifier"), "left_outer")
      .withColumn("kvalue", when(col("flag") === 1 && col("kvalue") < col("bottom_k"), col("bottom_k")).otherwise(when(col("flag") === 1 && col("kvalue") > col("top_k"), col("top_k")).otherwise(col("kvalue"))))

//    result
//        .withColumn("date", lit(date))
//        .withColumn("hour", lit(hour))
//        .withColumn("version", lit(version))
//        .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_check_smooth_k")

    println("k smooth strat1:")
    result.show(10)

    val resultDF = result.select("identifier", "kvalue", "conversion_goal")
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
    val regressionK = getModelK(conversionGoal, version, "regression", date, hour, spark).withColumn("regression_k", col("kvalue"))
    val pidK = getModelK(conversionGoal, version, "pid", date, hour, spark).withColumn("pid_k", col("kvalue"))
    val prevPb = getPrevPb(conversionGoal, version, date, hour, spark)
    val ocpcK = calculateKocpc(regressionK, pidK, prevPb, spark)

    // cpc投放的k值
    val cpcK1raw = getCpcK(mediaSelection, conversionGoal, 3, date, hour, spark)
    val cpcK2raw = getCpcKv2(mediaSelection, conversionGoal, date, hour, spark)
    val cpcK1 = cpcK1raw
      .withColumn("kvalue1", col("kvalue"))
      .select("identifier", "kvalue1", "pre_cvr", "post_cvr", "click", "conversion", "history_ocpc_flag")
    val cpcK2 = cpcK2raw
      .withColumn("kvalue2", col("kvalue"))
      .select("identifier", "kvalue2")
    val cpcKraw = cpcK1
      .join(cpcK2, Seq("identifier"), "left_outer")
    cpcKraw.write.mode("overwrite").saveAsTable("test.ocpc_check_k_by_conf20190320")
    val cpcK = cpcKraw
      .withColumn("kvalue", when(col("kvalue2").isNotNull, col("kvalue2")).otherwise(col("kvalue1")))
      .select("identifier", "kvalue", "pre_cvr", "post_cvr", "click", "conversion", "history_ocpc_flag")

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
      .withColumn("kvalue", when(col("history_ocpc_flag") === 0, col("cpc_k")).otherwise(col("ocpc_k")))
      .withColumn("conversion_goal", lit(conversionGoal))

    val resultDF = finalK.select("identifier", "kvalue", "conversion_goal")
//    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_check_smooth_k20190301b")

    resultDF

  }

  def getCpcKv2(mediaSelection: String, conversionGoal: Int, date: String, hour: String, spark: SparkSession) = {
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "ocpc_all.unitid_abtest_path"
    val path = conf.getString(conf_key)

    val unitidList = spark
      .read.format("json").json(path)
      .select("unitid", "conversion_goal")
      .filter(s"conversion_goal = $conversionGoal")
      .selectExpr("cast(unitid as string) identifier")


    val cpcK = getCpcK(mediaSelection, conversionGoal, 1, date, hour, spark)
    val resultDF = cpcK
      .join(unitidList, Seq("identifier"), "inner")
      .select("identifier", "kvalue")

    resultDF
  }

  def getCpcK(mediaSelection: String, conversionGoal: Int, dayCnt: Int, date: String, hour: String, spark: SparkSession) = {
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
    calendar.add(Calendar.DATE, -dayCnt)
    val dt = calendar.getTime
    val date1 = sdf.format(dt)
    val selectCondition = getTimeRangeSql3(date1, hour, date, hour)
    val selectCondition2 = getTimeRangeSql2(date1, hour, date, hour)

//    calendar.add(Calendar.DATE, -4)
//    val dt2 = calendar.getTime
//    val date2 = sdf.format(dt2)
//    val selectCondition2 = getTimeRangeSql2(date2, hour, date, hour)

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
    var hourCnt=1
    var prevTable = getPrevK(conversionGoal, version, date, hour, hourCnt, spark)
    while (hourCnt < 11) {
      val cnt = prevTable.count()
      println(s"check prevTable Count: $cnt, at hourCnt = $hourCnt")
      if (cnt>0) {
        hourCnt = 11
      } else {
        hourCnt += 1
        prevTable = getPrevK(conversionGoal, version, date, hour, hourCnt, spark)
      }

    }

    val resultDF = prevTable
      .select("identifier", "prev_k", "flag")

    resultDF
  }

  def getPrevK(conversionGoal: Int, version: String, date: String, hour: String, hourCnt: Int, spark: SparkSession) = {
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
    val selectCondition1 = s"`date`='$date1' and `hour`='$hour1' and conversion_goal=$conversionGoal and version='$version'"
    val selectCondition2 = getTimeRangeSql2(date1, hour1, date, hour)

    // 获取上一次的pb文件中的k值
    val sqlRequest1 =
      s"""
         |SELECT
         |  identifier,
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
      .select("identifier", "prev_k", "ctrcnt")
      .na.fill(0, Seq("ctrcnt"))
      .withColumn("flag", when(col("ctrcnt")>0, 1).otherwise(0))
      .withColumn("date", lit(date1))
      .withColumn("hour", lit(hour1))

    prevTable
  }


  def getModelK(conversionGoal: Int, version: String, method: String, date: String, hour: String, spark: SparkSession) = {
    val selectCondition = s"`date`='$date' and `hour`='$hour' and version='$version' and conversion_goal=$conversionGoal and method='$method'"
    val sqlRequest =
      s"""
         |SELECT
         |  identifier,
         |  kvalue
         |FROM
         |  dl_cpc.ocpc_k_model_hourly
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
      .table("dl_cpc.ocpc_filter_unionlog")
      .where(selectCondition)
      .filter(mediaSelection)
      .filter(s"is_ocpc = 1")
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

}


