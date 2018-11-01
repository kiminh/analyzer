package com.cpc.spark.ocpc

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.common.Utils.getTimeRangeSql
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import com.cpc.spark.udfs.Udfs_wj._
import org.apache.spark.sql.functions._
import sun.java2d.loops.DrawGlyphListAA


object OcpcPIDwithCPA {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("OcpcPIDwithCPA").enableHiveSupport().getOrCreate()

    val date = args(0).toString
    val hour = args(1).toString
    val isTest = args(2).toInt

    if (isTest == 1) {
      calculateKv1(date, hour, spark)
    } else {
      println("############## entering test stage ###################")
      // 初始化K值
      val testKstrat = calculateKv2(date, hour, spark)
    }


  }

  def checkKeffect(date: String, hour: String, spark: SparkSession): DataFrame ={
    /**
      * 读取历史数据：前四个小时的unionlog然后计算目前k值所占比例，比例低于阈值，返回0，否则返回1
      * 算法：
      * 1. 获取时间区间
      * 2. 从unionlog中抽取相关字段数据
      * 3. 抽取关键字段数据（ideaid, adclass, k）
      * 4. 计算各个k值的相对数量
      * 5. 从pb的历史数据表中抽取k值，找到该ideaid, adclass的k值的相对比例
      */

    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -4)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql(date1, hour1, date, hour)

    // 从unionlog中抽取相关字段数据
    val sqlRequest =
      s"""
         |SELECT
         |    ideaid,
         |    adclass,
         |    ocpc_log,
         |    exptags,
         |    date,
         |    hour
         |FROM
         |    dl_cpc.ocpc_result_unionlog_table_bak
         |WHERE
         |    $selectCondition
       """.stripMargin
    println(sqlRequest)

    val rawData = spark.sql(sqlRequest)
    rawData.write.mode("overwrite").saveAsTable("test.raw_data_check_k")

    // 抽取关键字段数据（ideaid, adclass, k）
    val model1Data = rawData
      .filter("exptags not like \"%ocpc_strategy:2%\"")
      .filter("ocpc_log != ''")
      .filter("split(split(ocpc_log, \",\")[7], \":\")[0]='kValue' OR split(split(ocpc_log, \",\")[7], \":\")[0]='kvalue'")


    //    model1Data.show(10)
    val modelDataWithK1 = model1Data.withColumn("k_value", udfMode1OcpcLogExtractCPA1()(col("ocpc_log"))).select("ideaid", "adclass", "k_value", "date", "hour")
    modelDataWithK1.show(10)
    modelDataWithK1.write.mode("overwrite").saveAsTable("test.ocpc_model_data_1")

    val model2Data = rawData
      .filter("exptags like \"%ocpc_strategy:2%\"")
      .filter("ocpc_log != ''")
      .filter("split(split(ocpc_log, \",\")[5], \":\")[0]='kValue' OR split(split(ocpc_log, \",\")[5], \":\")[0]='kvalue' OR split(ocpc_log, \",\")[5] is not null")


    //    model2Data.show(10)
    val modelDataWithK2 = model2Data.withColumn("k_value", udfModelOcpcLogExtractCPA2()(col("ocpc_log"))).select("ideaid", "adclass", "k_value", "date", "hour")
    modelDataWithK2.show(10)
    modelDataWithK2.write.mode("overwrite").saveAsTable("test.ocpc_model_data_2")

    val modelData = modelDataWithK1.union(modelDataWithK2)

    // 计算各个k值的相对数量
    val groupbyData = modelData.groupBy("ideaid", "adclass", "k_value").count()
    val totalCount = modelData.groupBy("ideaid", "adclass").count()
    groupbyData.select("ideaid", "adclass", "k_value", "count").createOrReplaceTempView("groupby_k_cnt")
    totalCount.select("ideaid", "adclass", "count").createOrReplaceTempView("groupby_totalcnt")
    val sqlRequest2 =
      s"""
         |SELECT
         |  a.ideaid,
         |  a.adclass,
         |  a.k_value,
         |  a.count as single_count,
         |  b.count as total_count,
         |  a.count * 1.0 / b.count as percent
         |FROM
         |  groupby_k_cnt as a
         |INNER JOIN
         |  groupby_totalcnt as b
         |ON
         |  a.ideaid=b.ideaid
         |AND
         |  a.adclass=b.adclass
       """.stripMargin

    println(sqlRequest2)

    val percentData = spark.sql(sqlRequest2)

    percentData.show(10)

    percentData.write.mode("overwrite").saveAsTable("test.ocpc_k_value_percent")



    // 从pb的历史数据表中抽取k值
    val dataDF = spark.table("test.new_pb_ocpc_with_pcvr").select("ideaid", "adclass", "k_value")

    percentData.createOrReplaceTempView("percent_k_value_table")
    dataDF.createOrReplaceTempView("previous_k_value_table")

    // 找到该ideaid, adclass的k值的相对比例,根据比例返回各自ideaid, adclass的flag，并生成结果表
    val sqlRequest3 =
      s"""
         |SELECT
         |  a.ideaid,
         |  a.adclass,
         |  a.k_value as exact_k,
         |  b.k_value as history_k,
         |  b.single_count,
         |  b.total_count,
         |  b.percent,
         |  (case when b.percent is null then 0
         |        when b.percent < 0.5 then 0
         |        else 1 end) flag
         |FROM
         |  previous_k_value_table as a
         |LEFT JOIN
         |  percent_k_value_table b
         |ON
         |  a.ideaid=b.ideaid
         |AND
         |  a.adclass=b.adclass
         |AND
         |  abs(a.k_value - b.k_value) < 0.001
       """.stripMargin

    println(sqlRequest3)

    val resultDF = spark.sql(sqlRequest3)

    resultDF.show(10)
    //
    //    resultDF.filter("history_k is not null").show(10)
    //
    //    resultDF.filter("flag=0").show(10)

    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_k_value_percent_flag")
    resultDF
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .write
      .mode("overwrite")
      .insertInto("dl_cpc.ocpc_k_value_percent_flag")

    resultDF


  }

  def testGenCPAgiven(filename: String, spark: SparkSession): DataFrame = {
    import spark.implicits._
    // 读取文件
    //    val data = spark.sparkContext.textFile(filename)

    // 生成cpa_given的rdd
    //    val resultRDD = data.map(x => (x.split(",")(0).toInt, x.split(",")(1).toInt))
    //    resultRDD.foreach(println)

    //    val resultDF = resultRDD.toDF("ideaid", "cpa_given")
    val resultDF = spark.table("test.ocpc_idea_update_time").select("ideaid", "cpa_given")
    resultDF
  }


  def genCPAratio(CPAgiven: DataFrame, date: String, hour: String, spark:SparkSession): Unit = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -4)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql(date1, hour1, date, hour)

    // read data and calculate cpa_history
    val sqlRequest =
      s"""
         |SELECT
         |    t.ideaid,
         |    (CASE WHEN t.cvr_total=0 then t.price_total * 10.0 else t.price_total * 1.0 / t.cvr_total end) as cpa_history
         |FROM
         |    (SELECT
         |        ideaid,
         |        SUM(cost) as price_total,
         |        SUM(cvr_cnt) as cvr_total
         |    FROM
         |        dl_cpc.ocpc_uid_userid_track_label2
         |    WHERE
         |        $selectCondition
         |    GROUP BY ideaid) t
       """.stripMargin
    println(sqlRequest)

    val CPAhistory = spark.sql(sqlRequest)
    CPAhistory.show(10)
    CPAhistory.printSchema()

    // 计算cpa_given和cpa_history的比值
    CPAgiven.createOrReplaceTempView("cpa_given_table")
    CPAhistory.createOrReplaceTempView("cpa_history_table")

    val sqlRequest2 =
      s"""
         |SELECT
         |  a.ideaid,
         |  a.cpa_given,
         |  b.cpa_history,
         |  (CASE WHEN b.cpa_history != 0 then a.cpa_given * 1.0 / b.cpa_history else a.cpa_given * 10.0 end) as ratio
         |FROM
         |  cpa_given_table a
         |INNER JOIN
         |  cpa_history_table b
         |ON
         |  a.ideaid=b.ideaid
       """.stripMargin

    println(sqlRequest2)

    val resultDF = spark.sql(sqlRequest2)
    resultDF.show(10)

    // 将结果输出到临时表
    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_cpa_given_history_ratio")

    resultDF
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .write
      .mode("overwrite")
      .insertInto("dl_cpc.ocpc_cpa_given_history_ratio")

  }

  def testCalculateK(date: String, hour: String, spark:SparkSession): Unit = {
    import spark.implicits._
    // 读取ocpc的k值来初始化
    val filename1="/user/cpc/wangjun/ocpc_k.txt"
    val data = spark.sparkContext.textFile(filename1)

    val dataRDD = data.map(x => (x.split(",")(0).toInt, x.split(",")(1).toString.slice(0,5)))
    //    dataRDD.foreach(println)

    val dataDF = dataRDD.toDF("ideaid", "k_value")
    dataDF.show(10)
    dataDF.write.mode("overwrite").saveAsTable("test.ocpc_k_value_init")

    //    val ratioDF = spark.table("test.ocpc_cpa_given_history_ratio")

    dataDF.createOrReplaceTempView("k_table")
    //    ratioDF.createOrReplaceTempView("ratio_table")

    val sqlRequest =
      s"""
         |SELECT
         |  a.ideaid,
         |  a.adclass,
         |  (case when c.k_value is null then 1.0
         |        when b.ratio is null and c.k_value is not null then c.k_value
         |        when b.ratio>1.0 and c.k_value is not null then c.k_value * 1.2
         |        when b.ratio<1.0 and c.k_value is not null then c.k_value / 1.2
         |        else c.k_value end) as k_value
         |FROM
         |  (SELECT ideaid, adclass, cast(k_value as double) as k_value FROM dl_cpc.ocpc_pb_result_table WHERE `date`='$date' and `hour`='$hour') as a
         |LEFT JOIN
         |  test.ocpc_cpa_given_history_ratio as b
         |ON
         |  a.ideaid=b.ideaid
         |LEFT JOIN
         |  (SELECT
         |      ideaid,
         |      k_value
         |   from
         |      test.ocpc_k_value_init
         |   group by ideaid, k_value) as c
         |ON
         |  a.ideaid=c.ideaid
       """.stripMargin

    println(sqlRequest)

    val resultDF = spark.sql(sqlRequest)

    println("final table of the k-value for ocpc:")
    resultDF.show(10)

    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_k_value_table")

  }



  def calculateK(dataset: DataFrame, spark:SparkSession): Unit = {
    import spark.implicits._

    val sqlRequest =
      s"""
         |SELECT
         |  a.ideaid,
         |  a.adclass,
         |  (case when a.k_value is null then 0.694
         |        when c.flag is null or c.flag = 0 then a.k_value
         |        when b.ratio is null then a.k_value
         |        when b.ratio > 1.0 and c.flag = 1 then a.k_value * 1.2
         |        when b.ratio < 1.0 and c.flag = 1 then a.k_value / 1.2
         |        else a.k_value end) as k_value
         |FROM
         |  (SELECT ideaid, adclass, cast(MIN(k_value) as double) as k_value FROM test.new_pb_ocpc_with_pcvr group by ideaid, adclass) as a
         |LEFT JOIN
         |  test.ocpc_cpa_given_history_ratio as b
         |ON
         |  a.ideaid=b.ideaid
         |LEFT JOIN
         |  (SELECT
         |      t.ideaid,
         |      t.adclass,
         |      t.flag
         |  FROM
         |      (SELECT
         |          ideaid,
         |          adclass,
         |          flag,
         |          row_number() over(partition by ideaid, adclass order by exact_k) as seq
         |      FROM
         |          test.ocpc_k_value_percent_flag) t
         |      WHERE
         |          t.seq=1) as c
         |ON
         |  a.ideaid=c.ideaid
         |AND
         |  a.adclass=c.adclass
       """.stripMargin

    println(sqlRequest)

    val resultDF = spark.sql(sqlRequest)

    println("final table of the k-value for ocpc:")
    resultDF.show(10)

    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_k_value_table")


  }

  def calculateKv1(date: String, hour: String, spark: SparkSession) ={
    val filename = "/user/cpc/wangjun/cpa_given.txt"

    // 读取cpa_given的text文件
    val dataset = testGenCPAgiven(filename, spark)
    dataset.show(10)
    // 计算CPA比值
    genCPAratio(dataset, date, hour, spark)

    // 确认是否需要修改k值
    val kFlags = checkKeffect(date, hour, spark)
    // 计算K值
    calculateK(kFlags, spark)

  }

  /*******************************************************************************/
  def calculateKv2(date: String, hour: String, spark: SparkSession) :DataFrame = {
    /**
      * 计算新版k值
      * 基于前6个小时的平均k值和那段时间的cpa_ratio，按照更加详细的分段函数对k值进行计算
      */

    val baseData = getBaseTable(date, hour, spark)
    println("################ baseData #################")
    baseData.show(10)
    val historyData = getHistoryData(date, hour, 6, spark)
    println("################# historyData ####################")
    historyData.show(10)
    val avgK = getAvgK(baseData, historyData, date, hour, 6, spark)
    println("################# avgK table #####################")
    avgK.show(10)
    val cpaRatio = getCPAratio(baseData, historyData, date, hour, 6, spark)
    println("################# cpaRatio table #######################")
    cpaRatio.show(10)
    val newK = updateKv2(baseData, avgK, cpaRatio, spark)
    println("################# final result ####################")
    newK.show(10)
    newK
  }

  def getBaseTable(endDate: String, hour: String, spark: SparkSession) :DataFrame ={
    // 计算日期周期
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val date = dateConverter.parse(endDate)
    val calendar = Calendar.getInstance
    calendar.setTime(date)
    calendar.add(Calendar.DATE, -7)
    val dt = calendar.getTime
    val startDate = dateConverter.format(dt)
    val selectCondition = getTimeRangeSql(startDate, hour, endDate, hour)

    // 累积计算最近一周数据
    val sqlRequest =
      s"""
         |SELECT
         |  ideaid,
         |  adclass
         |FROM
         |  dl_cpc.ocpc_uid_userid_track_label2
         |WHERE $selectCondition
         |GROUP BY ideaid, adclass
       """.stripMargin
    println(sqlRequest)
    val baseData = spark.sql(sqlRequest)

    // TODO 删除临时表
    baseData.write.mode("overwrite").saveAsTable("test.ocpc_base_table_hourly")
    baseData

  }

  def getHistoryData(date: String, hour: String, hourCnt: Int, spark: SparkSession) :DataFrame ={
    /**
      * 按照给定的时间区间获取从OcpcMonitor程序的结果表获取历史数据
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
    val selectCondition = getTimeRangeSql(date1, hour1, date, hour)

    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  ideaid,
         |  adclass,
         |  isshow,
         |  isclick,
         |  iscvr,
         |  price,
         |  ocpc_log,
         |  hour
         |FROM
         |  dl_cpc.ocpc_result_unionlog_table_bak
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)
    resultDF
  }

  def getAvgK(baseData: DataFrame, historyData: DataFrame, date: String, hour: String, hourCnt: Int, spark: SparkSession) :DataFrame ={
    /**
      * 计算修正前的k基准值
      * case1：前6个小时有isclick=1的数据，统计这批数据的k均值作为基准值
      * case2：前6个小时没有isclick=1的数据，将前一个小时的数据作为基准值
      * case3: 在主表（7*24）中存在，但是不属于前两种情况的，初始值0.694
      */

    historyData
      .withColumn("ocpc_log_dict", udfStringToMap()(col("ocpc_log")))
      .createOrReplaceTempView("raw_table")

    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  ideaid,
         |  adclass,
         |  isshow,
         |  isclick,
         |  iscvr,
         |  ocpc_log,
         |  ocpc_log_dict['kvalue'] as kvalue,
         |  hour
         |FROM
         |  raw_table
       """.stripMargin
    println(sqlRequest2)
    val rawData = spark.sql(sqlRequest2)

    // case1
    val case1 = rawData
      .filter("isclick=1")
      .groupBy("ideaid", "adclass")
      .agg(avg(col("kvalue")).alias("kvalue1")).select("ideaid", "adclass", "kvalue1")

    // case2
    // table name for previous calculation: test.new_pb_ocpc_with_pcvr
    val case2 = spark
      .table("test.new_pb_ocpc_with_pcvr")
      .withColumn("kvalue2", col("k_value"))
      .select("ideaid", "adclass", "kvalue2")

    // 优先case1，然后case2，最后case3
    val resultDF = baseData
      .join(case1, Seq("ideaid", "adclass"), "left_outer")
      .select("ideaid", "adclass", "kvalue1")
      .join(case2, Seq("ideaid", "adclass"), "left_outer")
      .select("ideaid", "adclass", "kvalue1", "kvalue2")
      .withColumn("kvalue_new", when(col("kvalue1").isNull, col("kvalue2")).otherwise(col("kvalue1")))
      .withColumn("kvalue", when(col("kvalue_new").isNull, 0.694).otherwise(col("kvalue_new")))

    resultDF.show(10)
    // TODO 删除临时表
    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_avg_k_value")
    resultDF

  }

  def getCPAratio(baseData: DataFrame, historyData: DataFrame, date: String, hour: String, hourCnt: Int, spark: SparkSession) :DataFrame ={
    /**
      * 计算前6个小时每个广告创意的cpa_given/cpa_real的比值
      * case1：前6个小时有cvr_cnt，按照定义计算比值即可
      * case2：前6个小时有ctr_cnt没有cvr_cnt，可能出价过高，需要降低k值，所以比值应该小于1
      * case3: 前6个小时没有ctr_cnt，可能出价过低，需要提高k值，所以比值应该大于1
      */

    // 获得cpa_given
    val cpaGiven = spark.table("test.ocpc_idea_update_time").select("ideaid", "cpa_given")

    // 按ideaid和adclass统计每一个广告创意的数据
    val rawData = historyData
      .withColumn("cost",
        when(col("isclick")===1,col("price")).otherwise(0))
      .groupBy("ideaid", "adclass")
      .agg(
        sum(col("cost")).alias("total_cost"),
        sum(col("isclick")).alias("ctr_cnt"),
        sum(col("iscvr")).alias("cvr_cnt"))
    // TODO 删除临时表
    rawData.write.mode("overwrite").saveAsTable("test.ocpc_ideai_cost_ctr_cvr")

    // 计算cpa_ratio
    val joinData = baseData
      .join(cpaGiven, Seq("ideaid"), "left_outer")
      .select("ideaid", "adclass", "cpa_given")
      .join(rawData, Seq("ideaid"), "left_outer")
      .select("ideaid", "adclass", "cpa_given", "total_cost", "ctr_cnt", "cvr_cnt")
    joinData.createOrReplaceTempView("join_table")



//    val joinData = cpaGiven
//      .join(rawData, Seq("ideaid"), "left_outer")
//      .select("ideaid", "adclass", "cpa_given", "total_cost", "ctr_cnt", "cvr_cnt")
//    joinData.createOrReplaceTempView("join_table")
    // TODO 删除临时表
    joinData.write.mode("overwrite").saveAsTable("test.ocpc_cpa_given_total_cost")

    // case1, case2, case3
    val sqlRequest =
      s"""
         |SELECT
         |  ideaid,
         |  adclass,
         |  cpa_given,
         |  total_cost,
         |  ctr_cnt,
         |  cvr_cnt,
         |  (case when cvr_cnt>0 and total_cost>0 then cpa_given * cvr_cnt * 1.0 / total_cost
         |        when cvr_cnt=0 and total_cost>0 then 0.8
         |        when cvr_cnt=0 and total_cost=0 then 1.2
         |        else 1.0 end) as cpa_ratio
         |FROM
         |  join_table
       """.stripMargin
    println(sqlRequest)
    val cpaRatio = spark.sql(sqlRequest)
    //TODO 删除临时表
    cpaRatio.write.mode("overwrite").saveAsTable("test.ocpc_cpa_ratio_v2")

    cpaRatio

  }

  def updateKv2(baseData: DataFrame, kValue: DataFrame, cpaRatio: DataFrame, spark: SparkSession) :DataFrame ={
    /**
      * 根据新的K基准值和cpa_ratio来在分段函数中重新定义k值
      * case1：0.9 <= cpa_ratio <= 1.1，k基准值
      * case2：0.8 <= cpa_ratio < 0.9，k / 1.1
      * case2：1.1 < cpa_ratio <= 1.2，k * 1.2
      * case3：0.6 <= cpa_ratio < 0.8，k / 1.2
      * case3：1.2 < cpa_ratio <= 1.4，k * 1.2
      * case4：0.4 <= cpa_ratio < 0.6，k / 1.4
      * case5：1.4 < cpa_ratio <= 1.6，k * 1.4
      * case6：cpa_ratio < 0.4，k / 1.6
      * case7：cpa_ratio > 1.6，k * 1.6
      *
      * 上下限依然是0.2 到1.2
      */

    // 关联得到基础表
    val rawData = baseData
      .join(kValue, Seq("ideaid", "adclass"), "left_outer")
      .select("ideaid", "adclass", "kvalue")
      .join(cpaRatio, Seq("ideaid", "adclass"), "left_outer")
      .select("ideaid", "adclass", "kvalue", "cpa_ratio")
    rawData.createOrReplaceTempView("raw_table")
    // TODO 删除临时表
    rawData.write.mode("overwrite").saveAsTable("test.ocpc_k_value_raw_table")

    // 按照分段函数修改k值
    val sqlRequest =
      s"""
         |SELECT
         |  ideaid,
         |  adclass,
         |  kvalue as prev_k,
         |  cpa_ratio,
         |  (case when kvalue is null then 0.694
         |        when cpa_ratio is null then cast(kvalue as double)
         |        when cpa_ratio between 0.9 and 1.1 then cast(kvalue as double)
         |        when cpa_ratio<0.9 and cpa_ratio>=0.8 then cast(kvalue as double) / 1.1
         |        when cpa_ratio>1.1 and cpa_ratio<=1.2 then cast(kvalue as double) * 1.1
         |        when cpa_ratio<0.8 and cpa_ratio>=0.6 then cast(kvalue as double) / 1.2
         |        when cpa_ratio>1.2 and cpa_ratio<=1.4 then cast(kvalue as double) * 1.2
         |        when cpa_ratio<0.6 and cpa_ratio>=0.4 then cast(kvalue as double) / 1.4
         |        when cpa_ratio>1.4 and cpa_ratio<=1.6 then cast(kvalue as double) * 1.4
         |        when cpa_ratio<0.4 then cast(kvalue as double) / 1.6
         |        when cpa_ratio>1.6 then cast(kvalue as double) * 1.6
         |        else cast(kvalue as double) end) as k_value
         |FROM
         |  raw_table
       """.stripMargin

    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)
    // TODO 删除临时表
    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_update_k_v2")
    resultDF
  }

}