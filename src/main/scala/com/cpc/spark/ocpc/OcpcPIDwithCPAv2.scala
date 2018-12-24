package com.cpc.spark.ocpc

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.common.Utils.getTimeRangeSql
import com.cpc.spark.ocpc.OcpcUtils._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import com.cpc.spark.udfs.Udfs_wj._
import org.apache.spark.sql.functions._
import sun.java2d.loops.DrawGlyphListAA


object OcpcPIDwithCPAv2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("OcpcPIDwithCPA").enableHiveSupport().getOrCreate()

    val date = args(0).toString
    val hour = args(1).toString
    val onDuty = args(2).toInt // onDuty=1表示部署模型，onDuty!=1表示测试新代码

    // TODO ideaid与userid的名称
    if (onDuty == 1) {
      val result = calculateKv3(date, hour, spark)
      result.write.mode("overwrite").saveAsTable("test.ocpc_k_value_table_bak")
//      result.write.mode("overwrite").saveAsTable("test.ocpc_k_value_table_" + hour)
    } else {
      println("############## entering test stage ###################")
      // 初始化K值
      //      val testKstrat = calculateKv2(date, hour, spark)
      //      calculateKv1(date, hour, spark)
      val testKstrat = calculateKv3(date, hour, spark)
    }


  }


  /*******************************************************************************/
  def calculateKv2(date: String, hour: String, spark: SparkSession) :DataFrame = {
    /**
      * 计算新版k值
      * 基于前6个小时的平均k值和那段时间的cpa_ratio，按照更加详细的分段函数对k值进行计算
      */

    val baseData = getBaseTable(date, hour, spark)
    println("################ baseData #################")
    //    baseData.show(10)
    val historyData = getHistoryData(date, hour, 6, spark)
    println("################# historyData ####################")
    //    historyData.show(10)
    val avgK = getAvgK(baseData, historyData, date, hour, spark)
    println("################# avgK table #####################")
    //    avgK.show(10)
    val cpaRatio = getCPAratio(baseData, historyData, date, hour, spark)
    println("################# cpaRatio table #######################")
    //    cpaRatio.show(10)
    val newK = updateKv2(baseData, avgK, cpaRatio, date, hour, spark)
    println("################# final result ####################")
    //    newK.show(10)
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
    //    baseData.write.mode("overwrite").saveAsTable("test.ocpc_base_table_hourly")
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
    val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)

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


  def getAvgK(baseData: DataFrame, historyData: DataFrame, date: String, hour: String, spark: SparkSession) :DataFrame ={
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
      .table("dl_cpc.new_pb_ocpc_with_pcvr")
      .withColumn("kvalue2", col("k_value"))
      .select("ideaid", "adclass", "kvalue2")
      .distinct()

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
    //    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_avg_k_value")
    resultDF

  }

  def getCPAratio(baseData: DataFrame, historyData: DataFrame, date: String, hour: String, spark: SparkSession) :DataFrame ={
    // TODO case
    /**
      * 计算前6个小时每个广告创意的cpa_given/cpa_real的比值
      * case1：hourly_ctr_cnt<10，可能出价过低，需要提高k值，所以比值应该大于1
      * case2：hourly_ctr_cnt>=10但是没有cvr_cnt，可能出价过高，需要降低k值，所以比值应该小于1
      * case3：hourly_ctr_cnt>=10且有cvr_cnt，按照定义计算比值即可
      */

    // 获得cpa_given
    val cpaGiven = spark.table("test.ocpc_idea_update_time_" + hour).select("ideaid", "cpa_given")

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
    //    rawData.write.mode("overwrite").saveAsTable("test.ocpc_ideaid_cost_ctr_cvr")

    // 计算单个小时的ctr_cnt和cvr_cnt
    val singleHour = historyData
      .filter(s"hour='$hour'")
      .groupBy("ideaid", "adclass").agg(sum("isclick").alias("hourly_ctr_cnt"), sum(col("iscvr")).alias("hourly_cvr_cnt"))
      .select("ideaid", "adclass", "hourly_ctr_cnt", "hourly_cvr_cnt")

    // 计算cpa_ratio
    val joinData = baseData
      .join(cpaGiven, Seq("ideaid"), "left_outer")
      .select("ideaid", "adclass", "cpa_given")
      .join(rawData, Seq("ideaid", "adclass"), "left_outer")
      .select("ideaid", "adclass", "cpa_given", "total_cost", "ctr_cnt", "cvr_cnt")
      .join(singleHour, Seq("ideaid", "adclass"), "left_outer")
      .select("ideaid", "adclass", "cpa_given", "total_cost", "ctr_cnt", "cvr_cnt", "hourly_ctr_cnt", "hourly_cvr_cnt")


    joinData.createOrReplaceTempView("join_table")

    //    val joinData = cpaGiven
    //      .join(rawData, Seq("ideaid"), "left_outer")
    //      .select("ideaid", "adclass", "cpa_given", "total_cost", "ctr_cnt", "cvr_cnt")
    //    joinData.createOrReplaceTempView("join_table")
    // TODO 删除临时表
    //    joinData.write.mode("overwrite").saveAsTable("test.ocpc_cpa_given_total_cost")



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
         |  (case when cpa_given is null then 1.0
         |        when '$hour'>'05' and (hourly_ctr_cnt<5 or hourly_ctr_cnt is null) then 1.2
         |        when hourly_ctr_cnt>=10 and (cvr_cnt=0 or cvr_cnt is null) then 0.8
         |        when cvr_cnt>0 then cpa_given * cvr_cnt * 1.0 / total_cost
         |        else 1.0 end) as cpa_ratio
         |FROM
         |  join_table
       """.stripMargin
    println(sqlRequest)
    val cpaRatio = spark.sql(sqlRequest)
    //TODO 删除临时表
    //    cpaRatio.write.mode("overwrite").saveAsTable("test.ocpc_cpa_ratio_v2")

    cpaRatio

  }

  def updateKv2(baseData: DataFrame, kValue: DataFrame, cpaRatio: DataFrame, date: String, hour: String, spark: SparkSession) :DataFrame ={
    /**
      * 根据新的K基准值和cpa_ratio来在分段函数中重新定义k值
      * case1：0.9 <= cpa_ratio <= 1.1，k基准值
      * case2：0.8 <= cpa_ratio < 0.9，k / 1.1
      * case2：1.1 < cpa_ratio <= 1.2，k * 1.1
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
    //    baseData.write.mode("overwrite").saveAsTable("test.ocpc_base_table")
    val rawData = baseData
      .join(kValue, Seq("ideaid", "adclass"), "left_outer")
      .select("ideaid", "adclass", "kvalue")
      .join(cpaRatio, Seq("ideaid", "adclass"), "left_outer")
      .select("ideaid", "adclass", "kvalue", "cpa_ratio_cvr2", "cpa_ratio_cvr3")
      .withColumn("ratio_tag_cvr2", udfSetRatioCase()(col("cpa_ratio_cvr2")))
      .withColumn("ratio_tag_cvr3", udfSetRatioCase()(col("cpa_ratio_cvr3")))
      .withColumn("updated_k2", udfUpdateK()(col("ratio_tag_cvr2"), col("kvalue")))
      .withColumn("updated_k3", udfUpdateK()(col("ratio_tag_cvr3"), col("kvalue")))

    rawData.createOrReplaceTempView("raw_table")
    // TODO 删除临时表
    rawData.write.mode("overwrite").saveAsTable("test.ocpc_k_value_raw_table_bak")
//    rawData
//      .withColumn("date", lit(date))
//      .withColumn("hour", lit(hour))
//      .write
//      .mode("overwrite")
//      .insertInto("dl_cpc.ocpc_k_value_raw_table")

//    val cvr3Data = getActivationData(date, hour, spark)

    // TODO 表名需要更换
//    new_pb_ocpc_with_pcvr
    val prevK = spark
      .table("dl_cpc.ocpc_pb_result_table_v5")
      .where(s"`date`='2018-12-24' and `hour`='11'")
      .groupBy("ideaid", "adclass")
      .agg(min(col("k_value")).alias("prevK"))
      .distinct()

    val resultDF = rawData
      .select("ideaid", "adclass", "updated_k2", "updated_k3")
      .join(prevK, Seq("ideaid", "adclass"), "left_outer")
      .select("ideaid", "adclass", "updated_k2", "updated_k3", "prevK")
      .withColumn("k_value2", when(col("updated_k2").isNull, col("prevK")).otherwise(col("updated_k2")))
      .withColumn("k_value3", when(col("updated_k3").isNull, col("prevK")).otherwise(col("updated_k3")))
      .select("ideaid", "adclass", "k_value2", "updated_k2", "k_value3", "updated_k3")
    //      .join(cvr3Data, Seq("ideaid"), "left_outer")
    //      .select("ideaid", "adclass", "new_k_value", "updated_k", "flag")
    //      .withColumn("k_value", when(col("flag").isNotNull, col("new_k_value") / 2.0).otherwise(col("new_k_value")))


    // TODO 删除临时表
    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_update_k_v2_bak")
    resultDF
  }

  /*******************************************************************/
  // TODO: 下一阶段优化目标，考虑24小时的cpa表现，按权重计算
  def calculateKv3(date: String, hour: String, spark: SparkSession) :DataFrame ={
    /**
      * 计算新版k值
      * 基于前6个小时的平均k值和那段时间的cpa_ratio，按照更加详细的分段函数对k值进行计算
      */

    val baseData = getBaseTable(date, hour, spark)
    println("################ baseData #################")
    //    baseData.show(10)
    val historyData = getHistoryData(date, hour, 24, spark)
    println("################# historyData ####################")
    //    historyData.show(10)
    val avgK = getAvgK(baseData, historyData, date, hour, spark)
    //    val avgK = getAvgKV3(baseData, historyData, date, hour, spark)
    println("################# avgK table #####################")
    //    avgK.show(10)
    val cpaRatio = getCPAratioV3(baseData, historyData, date, hour, spark)
    println("################# cpaRatio table #######################")
    //    cpaRatio.show(10)
    val newK = updateKv2(baseData, avgK, cpaRatio, date, hour, spark)
    println("################# final result ####################")
    //    newK.show(10)
    newK

  }

  def getCPAratioV3(baseData: DataFrame, historyData: DataFrame, date: String, hour: String, spark: SparkSession) :DataFrame = {
    /**
      * 按照权值在24h的时间窗口内计算cpa_ratio
      * case1：hourly_ctr_cnt<10，可能出价过低，需要提高k值，所以比值应该大于1
      * case2：hourly_ctr_cnt>=10但是没有cvr_cnt，可能出价过高，需要降低k值，所以比值应该小于1
      * case3：hourly_ctr_cnt>=10且有cvr_cnt，按照定义计算比值即可
      */

    // 按ideaid和adclass统计每一个广告创意的加权数据
    val rawData = historyData
      .withColumn("cost",
        when(col("isclick")===1, col("price")).otherwise(0))
      .groupBy("ideaid", "adclass", "hour")
      .agg(
        sum(col("cost")).alias("cost"),
        sum(col("isclick")).alias("ctr_cnt"),
        sum(col("iscvr")).alias("cvr_cnt"))
      .withColumn("weight", udfCalculateWeightByHour(hour)(col("hour")))
      .withColumn("weighted_cost", col("cost") * col("weight"))
      .withColumn("weighted_cvr_cnt", col("cvr_cnt") * col("weight"))
      .groupBy("ideaid", "adclass")
      .agg(
        sum(col("weighted_cost")).alias("total_cost"),
        sum(col("weighted_cvr_cnt")).alias("cvr_cnt"))

    // TODO 删除临时表
    //    rawData.write.mode("overwrite").saveAsTable("test.ocpc_ideaid_cost_ctr_cvr_v3")

    // 获得cpa_given
    val cpaGiven = spark.table("test.ocpc_idea_update_time_" + hour).select("ideaid", "cpa_given")

    // 计算单个小时的ctr_cnt和cvr_cnt
    val singleHour = historyData
      .filter(s"hour='$hour'")
      .groupBy("ideaid", "adclass").agg(sum("isclick").alias("hourly_ctr_cnt"), sum(col("iscvr")).alias("hourly_cvr_cnt"))
      .select("ideaid", "adclass", "hourly_ctr_cnt", "hourly_cvr_cnt")




    // 计算cpa_ratio
    val joinData = baseData
      .join(cpaGiven, Seq("ideaid"), "left_outer")
      .select("ideaid", "adclass", "cpa_given")
      .join(rawData, Seq("ideaid", "adclass"), "left_outer")
      .select("ideaid", "adclass", "cpa_given", "total_cost", "cvr_cnt")
      .join(singleHour, Seq("ideaid", "adclass"), "left_outer")
      .select("ideaid", "adclass", "cpa_given", "total_cost", "cvr_cnt", "hourly_ctr_cnt", "hourly_cvr_cnt")


    joinData.createOrReplaceTempView("join_table")

    // TODO 删除临时表
    //    joinData.write.mode("overwrite").saveAsTable("test.ocpc_cpa_given_total_cost_v3")

    // case1, case2, case3
    //    val sqlRequest =
    //      s"""
    //         |SELECT
    //         |  ideaid,
    //         |  adclass,
    //         |  cpa_given,
    //         |  total_cost,
    //         |  cvr_cnt,
    //         |  (case when cpa_given is null then 1.0
    //         |        when '$hour'>'05' and (hourly_ctr_cnt<5 or hourly_ctr_cnt is null) then -1
    //         |        when hourly_ctr_cnt>=10 and (cvr_cnt=0 or cvr_cnt is null) then 0.8
    //         |        when cvr_cnt>0 then cpa_given * cvr_cnt * 1.0 / total_cost
    //         |        else 1.0 end) as cpa_ratio_cvr2
    //         |FROM
    //         |  join_table
    //       """.stripMargin

    val sqlRequest =
      s"""
         |SELECT
         |  ideaid,
         |  adclass,
         |  cpa_given,
         |  total_cost,
         |  cvr_cnt,
         |  (case when cpa_given is null then 1.0
         |        when cvr_cnt=0 or cvr_cnt is null then 0.8
         |        when cvr_cnt>0 then cpa_given * cvr_cnt * 1.0 / total_cost
         |        else 1.0 end) as cpa_ratio_cvr2
         |FROM
         |  join_table
       """.stripMargin
    println(sqlRequest)
    val cpaRatioCvr2 = spark.sql(sqlRequest)


    //    val ideaBalance = getCurrentBudget(date, hour, spark)
    val cpaRatioCvr3 = getAPIcvr3V3(singleHour, date, hour, spark)

    val cpaRatio = cpaRatioCvr2
      .join(cpaRatioCvr3, Seq("ideaid", "adclass"), "left_outer")
//      .withColumn("cpa_ratio", when(col("flag").isNotNull, col("cpa_ratio_cvr3")).otherwise(col("cpa_ratio_cvr2")))




    //TODO 删除临时表
    cpaRatio.write.mode("overwrite").saveAsTable("test.ocpc_cpa_ratio_v3_bak")
//    cpaRatio
//      .withColumn("date", lit(date))
//      .withColumn("hour", lit(hour))
//      .write
//      .mode("overwrite")
//      .insertInto("dl_cpc.ocpc_check_cpa_ratio_hourly")

    cpaRatio

  }

  def getActivationData(date: String, hour: String, spark: SparkSession) = {
    import spark.implicits._

    val resultDF = spark
      .table("test.ocpc_idea_update_time_" + hour)
      .filter("conversion_goal=2")
      .withColumn("flag", lit(1))
      .select("ideaid", "cpa_given", "flag")


    resultDF
  }

  //TODO 给api回传模型做反馈机制
  def getAPIcvr3V3(singleHour: DataFrame, date: String, hour: String, spark: SparkSession) :DataFrame = {
    val cvr3List = getActivationData(date, hour, spark)

    val cvr3Data = getActData(date, hour, 24, spark)

    val rawData = cvr3Data
      .groupBy("ideaid", "adclass")
      .agg(
        sum(col("cost")).alias("cvr3_cost"),
        sum(col("cvr_cnt")).alias("cvr3_cvr_cnt"))
      .select("ideaid", "adclass", "cvr3_cost", "cvr3_cvr_cnt")



    val historyData = getCompleteHistoryData(date, hour, 24, spark)
    val costData = historyData
      .groupBy("ideaid", "adclass")
      .agg(sum(col("cost")).alias("cost"))
      .select("ideaid", "adclass", "cost")

    // 计算单个小时的ctr_cnt和cvr_cnt
    //    singleHour.write.mode("overwrite").saveAsTable("test.test_ocpc_cvr3_debug_singlehour")

    val data = cvr3List
      .join(costData, Seq("ideaid"), "left_outer")
      .select("ideaid", "adclass", "cost", "cpa_given", "flag")
      .join(rawData, Seq("ideaid", "adclass"), "left_outer")
      .select("ideaid", "adclass", "cost", "cvr3_cost", "cvr3_cvr_cnt", "cpa_given", "flag")
      .join(singleHour, Seq("ideaid", "adclass"), "left_outer")
      .select("ideaid", "adclass", "cost", "cvr3_cost", "cvr3_cvr_cnt", "cpa_given", "flag", "hourly_ctr_cnt")



    data.createOrReplaceTempView("data_table")
    //    val sqlRequest =
    //      s"""
    //         |SELECT
    //         |  ideaid,
    //         |  adclass,
    //         |  cost,
    //         |  cpa_given,
    //         |  cvr3_cvr_cnt,
    //         |  hourly_ctr_cnt,
    //         |  flag,
    //         |  (case when cpa_given is null then 1.0
    //         |        when '$hour'>'05' and (hourly_ctr_cnt<5 or hourly_ctr_cnt is null) then -1
    //         |        when hourly_ctr_cnt>=10 and (cvr3_cvr_cnt=0 or cvr3_cvr_cnt is null) then 0.8
    //         |        when cvr3_cvr_cnt>0 then cpa_given * cvr3_cvr_cnt * 1.0 / cost
    //         |        else 1.0 end) as cpa_ratio_cvr3
    //         |FROM
    //         |  data_table
    //       """.stripMargin

    val sqlRequest =
      s"""
         |SELECT
         |  ideaid,
         |  adclass,
         |  cost,
         |  cpa_given,
         |  cvr3_cvr_cnt,
         |  hourly_ctr_cnt,
         |  flag,
         |  (case when cpa_given is null then 1.0
         |        when cvr3_cvr_cnt=0 or cvr3_cvr_cnt is null then 0.8
         |        when cvr3_cvr_cnt>0 then cpa_given * cvr3_cvr_cnt * 1.0 / cost
         |        else 1.0 end) as cpa_ratio_cvr3
         |FROM
         |  data_table
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)


    // TODO 删除临时表
    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_pid_with_cvr3_k_bak")
    val finalDF = resultDF.select("ideaid", "adclass", "flag", "cvr3_cvr_cnt", "cpa_ratio_cvr3")
    finalDF
  }

  def getCurrentBudget(date: String, hour: String, spark: SparkSession) :DataFrame = {
    val sqlRequest =
      s"""
         |SELECT
         |    t.ideaid,
         |    t.balance
         |FROM
         |    (SELECT
         |        ideaid,
         |        least_xbalance as balance,
         |        row_number() over(partition by ideaid order by least_xbalance) as seq
         |    FROM
         |        dl_cpc.ocpc_ideaid_budget
         |    WHERE
         |        `date`='$date'
         |    AND
         |        `hour`='$hour') as t
         |WHERE
         |    t.seq=1
       """.stripMargin

    val data = spark.sql(sqlRequest)

    val resultDF = data
      .withColumn("balance_label", when(col("balance") < 100, 1).otherwise(0))
      .select("ideaid", "balance_label")

    resultDF
  }

  def getAvgKV3(baseData: DataFrame, historyData: DataFrame, date: String, hour: String, spark: SparkSession) :DataFrame ={
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
      .groupBy("ideaid", "adclass", "hour")
      .agg(
        sum(col("kvalue")).alias("hourly_k"),
        sum(col("isclick")).alias("hourly_ctr_cnt"))
      .filter("hourly_k>0")
      .withColumn("weight", udfCalculateWeightByHour(hour)(col("hour")))
      .withColumn("weighted_k", col("weight")*col("hourly_k"))
      .withColumn("weighted_ctr_cnt", col("weight")*col("hourly_ctr_cnt"))
      .groupBy("ideaid", "adclass")
      .agg(
        sum(col("weighted_k")).alias("total_k"),
        sum(col("weighted_ctr_cnt")).alias("total_ctr_cnt"))
      .withColumn("kvalue1", col("total_k") / col("total_ctr_cnt"))
      .select("ideaid", "adclass", "kvalue1")

    //TODO 删除case3
    val case3 = rawData
      .filter("isclick=1")
      .groupBy("ideaid", "adclass")
      .agg(avg(col("kvalue")).alias("kvalue3")).select("ideaid", "adclass", "kvalue3")



    //      .agg(avg(col("kvalue")).alias("kvalue1")).select("ideaid", "adclass", "kvalue1")

    // case2
    // table name for previous calculation: test.new_pb_ocpc_with_pcvr
    val case2 = spark
      .table("dl_cpc.new_pb_ocpc_with_pcvr")
      .withColumn("kvalue2", col("k_value"))
      .select("ideaid", "adclass", "kvalue2")
      .distinct()

    // 优先case1，然后case2，最后case3
    // TODO 删除case3
    val resultDF = baseData
      .join(case1, Seq("ideaid", "adclass"), "left_outer")
      .select("ideaid", "adclass", "kvalue1")
      .join(case2, Seq("ideaid", "adclass"), "left_outer")
      .select("ideaid", "adclass", "kvalue1", "kvalue2")
      .withColumn("kvalue_new", when(col("kvalue1").isNull, col("kvalue2")).otherwise(col("kvalue1")))
      .withColumn("kvalue", when(col("kvalue_new").isNull, 0.694).otherwise(col("kvalue_new")))
      .join(case3, Seq("ideaid", "adclass"), "left_outer")

    resultDF.show(10)
    // TODO 删除临时表
    //    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_avg_k_value_v3")
    resultDF

  }

}
