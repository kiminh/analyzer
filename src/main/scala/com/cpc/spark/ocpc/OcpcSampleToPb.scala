

package com.cpc.spark.ocpc

import java.text.SimpleDateFormat
import java.util.Calendar

import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer
import userocpc.userocpc._
import java.io.FileOutputStream

import com.cpc.spark.common.Utils.getTimeRangeSql
import com.cpc.spark.ocpc.OcpcUtils.{getActData, getTimeRangeSql2, getTimeRangeSql3}
import com.cpc.spark.ocpc.utils.OcpcUtils._
import org.apache.spark.sql.functions._



object OcpcSampleToPb {
  def main(args: Array[String]): Unit = {
    /*
    分成4个部分构成pb文件:
    1. 基础部分：ideaid, userid, adclass, cost, ctrcnt, cvrcnt, adclass_cost, adclass_ctrcnt, adclass_cvrcnt
    2. 历史预测转化率: hpcvr
    3. api回传类历史转化数
    4. k值： k2, k3
     */
    val date = args(0).toString
    val hour = args(1).toString
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    val baseData = getBaseData(date,hour, spark)
    val hpcvr = getHpcvr(date, hour, spark)
    val apiCvr = getAPIcvr(date, hour, spark)
    val resultK = getK(date, hour, spark)
    val ocpcSuggest = getOcpcSuggest(date, hour, spark)

    val currentPb = baseData
      .join(hpcvr, Seq("ideaid", "adclass"), "left_outer")
      .join(apiCvr, Seq("ideaid", "adclass"), "left_outer")
      .join(resultK, Seq("ideaid", "adclass"), "left_outer")
      .select("ideaid", "userid", "adclass", "cost", "ctr_cnt", "cvr_cnt", "adclass_cost", "adclass_ctr_cnt", "adclass_cvr_cnt", "hpcvr", "cvr3_cnt", "k_value2", "k_value3")
      .withColumn("kvalue1_init", col("k_value2"))
      .withColumn("kvalue2_init", col("k_value3"))


    val result1 = initKv2(currentPb, date, hour,spark)
//    val resultTmp = initK(currentPb, date, hour, spark)
//    result1.write.mode("overwrite").saveAsTable("test.ocpc_qtt_prev_pb20190129a")
//    resultTmp.write.mode("overwrite").saveAsTable("test.ocpc_qtt_prev_pb20190129b")
    val result2 = assemblyPB(result1, date, hour, spark)
    val result3 = processCPAsuggest(result2, ocpcSuggest, date, hour, spark)
    val resultDF = getCPCbid(result3, date, hour, spark)


    resultDF.write.mode("overwrite").saveAsTable("dl_cpc.ocpc_qtt_prev_pb")
    resultDF
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_pb_result_table_v7")
//    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_qtt_prev_pb20190129")

    savePbPack(resultDF)

  }

  def getCPCbid(data: DataFrame, date: String, hour: String, spark: SparkSession) = {
    import spark.implicits._
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -2)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)
    val selectCondition = getTimeRangeSql2(date1, hour, date, hour)
    println(selectCondition)

    val bidData1 = spark
      .table("dl_cpc.filtered_union_log_bid_hourly")
      .where(selectCondition)
      .filter(s"media_appsid in ('80000001', '80000002')")
      .filter(s"length(ocpc_log) > 0")
      .select("ideaid", "bid")
      .groupBy("ideaid")
      .agg(avg(col("bid")).alias("cpc_bid1"))
      .select("ideaid", "cpc_bid1")

    // 读取实验ideaid列表
    val filename = "/user/cpc/wangjun/ocpc_bid_ideas.txt"
    val expData = spark.sparkContext.textFile(filename)
    val rawRDD = expData.map(x => (x.split(",")(0).toInt, x.split(",")(1).toDouble))
    rawRDD.foreach(println)
    val bidData2 = rawRDD
      .toDF("ideaid", "cpc_bid2")
      .groupBy("ideaid")
      .agg(avg(col("cpc_bid2")).alias("cpc_bid2"))


    // 读取实验ideaid列表
    val filename2 = "/user/cpc/wangjun/ocpc_ab_ideas.txt"
    val expData2 = spark.sparkContext.textFile(filename2)
    val rawRDD2 = expData2.map(x => (x.split(",")(0).toInt, x.split(",")(1).toInt))
    rawRDD2.foreach(println)
    val bidDataIdeas = rawRDD2
      .toDF("ideaid", "flag")
      .filter(s"flag=1")
      .distinct()

    val bidData = bidDataIdeas
      .join(bidData1, Seq("ideaid"), "left_outer")
      .join(bidData2, Seq("ideaid"), "left_outer")
      .select("ideaid", "cpc_bid1", "cpc_bid2")
      .withColumn("cpc_bid", when(col("cpc_bid2").isNull, col("cpc_bid1")).otherwise(col("cpc_bid2")))
      .select("ideaid", "cpc_bid")
      .filter(s"cpc_bid is not null")


    // 数据关联
    val resultDF = data
      .join(bidData, Seq("ideaid"), "left_outer")
      .na.fill(0.0, Seq("cpc_bid"))
      .select("ideaid", "userid", "adclass", "cost", "ctr_cnt", "cvr_cnt", "adclass_cost", "adclass_ctr_cnt", "adclass_cvr_cnt", "k_value", "hpcvr", "cali_value", "cvr3_cali", "cvr3_cnt", "kvalue1", "kvalue2", "t", "cpa_suggest", "cpc_bid")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    resultDF
  }

  def processCPAsuggest(data: DataFrame, ocpcSuggest: DataFrame, date: String, hour: String, spark: SparkSession) = {
//    val rawData = data
//      .withColumn("conversion_goal", when(col("k_value")===col("kvalue2"), 2).otherwise(1))

    val rawData1 = data.filter(s"k_value=kvalue1")
    val rawData2 = data.filter(s"k_value!=kvalue1")
    val ocpcSuggest1 = ocpcSuggest
      .filter("conversion_goal=1")
      .withColumn("t1", col("t"))
      .withColumn("cpa_suggest1", col("cpa_suggest"))
      .select("ideaid", "t1", "cpa_suggest1")
    val ocpcSuggest2 = ocpcSuggest
      .filter("conversion_goal=2")
      .withColumn("t2", col("t"))
      .withColumn("cpa_suggest2", col("cpa_suggest"))
      .select("ideaid", "t2", "cpa_suggest2")
    val ocpcSuggest3 = ocpcSuggest
      .filter("conversion_goal=3")
      .withColumn("t3", col("t"))
      .withColumn("cpa_suggest3", col("cpa_suggest"))
      .select("ideaid", "t3", "cpa_suggest3")

    val data1 = rawData1
      .join(ocpcSuggest1, Seq("ideaid"), "left_outer")
      .join(ocpcSuggest3, Seq("ideaid"), "left_outer")
      .select("ideaid", "userid", "adclass", "cost", "ctr_cnt", "cvr_cnt", "adclass_cost", "adclass_ctr_cnt", "adclass_cvr_cnt", "k_value", "hpcvr", "cali_value", "cvr3_cali", "cvr3_cnt", "kvalue1", "kvalue2", "t1", "cpa_suggest1", "t3", "cpa_suggest3")
      .withColumn("t", when(col("cpa_suggest3").isNotNull, col("t3")).otherwise(col("t1")))
      .withColumn("cpa_suggest", when(col("cpa_suggest3").isNotNull, col("cpa_suggest3")).otherwise(col("cpa_suggest1")))
      .withColumn("t", when(col("cpa_suggest").isNull, 0.0).otherwise(col("t")))
      .na.fill(0.0, Seq("t", "cpa_suggest"))
      .select("ideaid", "userid", "adclass", "cost", "ctr_cnt", "cvr_cnt", "adclass_cost", "adclass_ctr_cnt", "adclass_cvr_cnt", "k_value", "hpcvr", "cali_value", "cvr3_cali", "cvr3_cnt", "kvalue1", "kvalue2", "t", "cpa_suggest")

    val data2 = rawData2
      .join(ocpcSuggest2, Seq("ideaid"), "left_outer")
      .withColumn("t", col("t2"))
      .withColumn("cpa_suggest", col("cpa_suggest2"))
      .withColumn("t", when(col("cpa_suggest").isNull, 0.0).otherwise(col("t")))
      .na.fill(0.0, Seq("t", "cpa_suggest"))
      .select("ideaid", "userid", "adclass", "cost", "ctr_cnt", "cvr_cnt", "adclass_cost", "adclass_ctr_cnt", "adclass_cvr_cnt", "k_value", "hpcvr", "cali_value", "cvr3_cali", "cvr3_cnt", "kvalue1", "kvalue2", "t", "cpa_suggest")

    val resultDF = data1.union(data2)

    resultDF

  }

  def getOcpcSuggest(date: String, hour: String, spark: SparkSession) = {
    val data = spark
      .table("dl_cpc.ocpc_cpa_suggest_once")
      .where(s"version='qtt_demo'")
      .select("ideaid", "conversion_goal", "t", "cpa_suggest")
      .groupBy("ideaid", "conversion_goal")
      .agg(avg(col("t")).alias("t"), avg(col("cpa_suggest")).alias("cpa_suggest"))
      .select("ideaid", "conversion_goal", "t", "cpa_suggest")

    data
  }



  def savePbPack(dataset: Dataset[Row]): Unit = {
    var list = new ListBuffer[SingleUser]
    val filename = s"UseridDataOcpc.pb"
    println("size of the dataframe")
    println(dataset.count)
    dataset.show(10)
    var cnt = 0

    for (record <- dataset.collect()) {
      val ideaid = record.get(0).toString
      val userId = record.get(1).toString
      val adclassId = record.get(2).toString
      val costValue = record.get(3).toString
      val ctrValue = record.getLong(4).toString
      val cvrValue = record.getLong(5).toString
      val adclassCost = record.get(6).toString
      val adclassCtr = record.getLong(7).toString
      val adclassCvr = record.getLong(8).toString
      val k = record.get(9).toString
      val hpcvr = record.getAs[Double]("hpcvr")
      val caliValue = record.getAs[Double]("cali_value")
      val cvr3Cali = record.getAs[Double]("cvr3_cali")
      val cvr3Cnt = record.getAs[Long]("cvr3_cnt")
      val k_value1 = record.getAs[Double]("kvalue1")
      val k_value2 = record.getAs[Double]("kvalue2")
      val min_bid = 0.2
      val cpa_suggest = record.getAs[Double]("cpa_suggest")
      var t_span = record.getAs[Double]("t")
      val cpc_bid = record.getAs[Double]("cpc_bid")
      if (t_span != 0.0) {
        t_span = 3.0
      }

      if (cnt % 500 == 0) {
        println(s"ideaid:$ideaid, userId:$userId, adclassId:$adclassId, costValue:$costValue, ctrValue:$ctrValue, cvrValue:$cvrValue, adclassCost:$adclassCost, adclassCtr:$adclassCtr, adclassCvr:$adclassCvr, k:$k, hpcvr:$hpcvr, caliValue:$caliValue, cvr3Cali:$cvr3Cali, cvr3Cnt:$cvr3Cnt, kvalue1:$k_value1, kvalue2:$k_value2, minBid:$min_bid, cpaSuggest:$cpa_suggest, t:$t_span, cpcBid:$cpc_bid")
      }
      cnt += 1

      val tmpCost = adclassCost.toLong
      if (tmpCost<0) {
        println("#######################################")
        println("negative cost")
        println(record)
      } else {
        val currentItem = SingleUser(
          ideaid = ideaid,
          userid = userId,
          cost = costValue,
          ctrcnt = ctrValue,
          cvrcnt = cvrValue,
          adclass = adclassId,
          adclassCost = adclassCost,
          adclassCtrcnt = adclassCtr,
          adclassCvrcnt = adclassCvr,
          kvalue = k,
          hpcvr = hpcvr,
          calibration = caliValue,
          cvr3Cali = cvr3Cali,
          cvr3Cnt = cvr3Cnt,
          kvalue1 = k_value1,
          kvalue2 = k_value2,
          minBid = min_bid,
          cpaSuggest = cpa_suggest,
          t = t_span,
          cpcBid = cpc_bid
        )
        list += currentItem

      }
    }
    val result = list.toArray[SingleUser]
    val useridData = UserOcpc(
      user = result
    )
    println("length of the array")
    println(result.length)
    useridData.writeTo(new FileOutputStream(filename))

    println("complete save data into protobuffer")

  }


  def getBaseData(date: String, hour: String, spark: SparkSession) = {
    // 计算日期周期
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val end_date = sdf.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(end_date)
    calendar.add(Calendar.DATE, -7)
    val dt = calendar.getTime
    val date1 = sdf.format(dt)
    val selectCondition = getTimeRangeSql(date1, hour, date, hour)

    // 累积计算最近一周数据
    val sqlRequest1 =
      s"""
         |SELECT
         |  userid,
         |  ideaid,
         |  adclass,
         |  date,
         |  hour,
         |  SUM(cost) as cost,
         |  SUM(ctr_cnt) as ctr_cnt,
         |  SUM(cvr_cnt) as cvr_cnt,
         |  SUM(total_cnt) as total_cnt
         |FROM
         |  dl_cpc.ocpc_uid_userid_track_label2
         |WHERE $selectCondition
         |GROUP BY userid, ideaid, adclass, date, hour
       """.stripMargin
    println(sqlRequest1)

    val rawBase = spark.sql(sqlRequest1)

    rawBase.createOrReplaceTempView("base_table")

    val base = rawBase.select("userid", "ideaid", "adclass", "cost", "ctr_cnt", "cvr_cnt", "total_cnt")

    // 按ideaid求和
    val userData = base
      .groupBy("userid", "ideaid", "adclass")
      .agg(
        sum("cost").alias("cost"),
        sum("ctr_cnt").alias("user_ctr_cnt"),
        sum("cvr_cnt").alias("user_cvr_cnt"))
      .select("ideaid", "userid", "adclass", "cost", "user_ctr_cnt", "user_cvr_cnt")

    userData.createOrReplaceTempView("ocpc_data_userdata")


    // 按adclass求和
    val adclassData = userData
      .groupBy("adclass")
      .agg(
        sum("cost").alias("adclass_cost"),
        sum("user_ctr_cnt").alias("adclass_ctr_cnt"),
        sum("user_cvr_cnt").alias("adclass_cvr_cnt"))
      .select("adclass", "adclass_cost", "adclass_ctr_cnt", "adclass_cvr_cnt")

    adclassData.createOrReplaceTempView("ocpc_data_adclassdata")


    // 关联adclass和ideaid
    val sqlRequest2 =
      s"""
         |SELECT
         |    a.ideaid,
         |    a.userid,
         |    a.adclass,
         |    a.cost,
         |    a.user_ctr_cnt as ctr_cnt,
         |    a.user_cvr_cnt as cvr_cnt,
         |    b.adclass_cost,
         |    b.adclass_ctr_cnt,
         |    b.adclass_cvr_cnt
         |FROM
         |    ocpc_data_userdata a
         |INNER JOIN
         |    ocpc_data_adclassdata b
         |ON
         |    a.adclass=b.adclass
       """.stripMargin
    println(sqlRequest2)
    val resultDF = spark
      .sql(sqlRequest2)
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    resultDF
  }

  def getHpcvr(date: String, hour: String, spark: SparkSession) = {
    // 计算日期周期
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val end_date = sdf.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(end_date)
    calendar.add(Calendar.DATE, -7)
    val dt = calendar.getTime
    val date1 = sdf.format(dt)
    val selectCondition = getTimeRangeSql(date1, hour, date, hour)

    // read data and set redis configuration
    val sqlRequest =
      s"""
         |SELECT
         |  ideaid,
         |  adclass,
         |  SUM(total_cvr) * 1.0 / SUM(cnt) as hpcvr
         |FROM
         |  dl_cpc.ocpc_pcvr_history
         |WHERE $selectCondition
         |GROUP BY ideaid, adclass
       """.stripMargin
    println(sqlRequest)

    val resultDF = spark
      .sql(sqlRequest)
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    resultDF
  }

  def getAPIcvr(date: String, hour: String, spark: SparkSession) = {
    // 取历史数据
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val end_date = sdf.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(end_date)
    calendar.add(Calendar.DATE, -7)
    val dt = calendar.getTime
    val date1 = sdf.format(dt)
    val selectCondition = getTimeRangeSql2(date1, hour, date, hour)

    // read data and set redis configuration
    val sqlRequest1 =
      s"""
         |SELECT
         |  ideaid,
         |  adclass,
         |  cost,
         |  ctr_cnt,
         |  cvr_cnt,
         |  hour
         |FROM
         |  dl_cpc.ocpc_ideaid_adclass_label3_track_v1
         |WHERE $selectCondition
       """.stripMargin
    println(sqlRequest1)
    val historyData = spark.sql(sqlRequest1)

    val resultDF = historyData
      .groupBy("ideaid", "adclass")
      .agg(sum(col("cvr_cnt")).alias("base_cvr3_cnt"))
      .select("ideaid", "adclass", "base_cvr3_cnt")
      .withColumn("cvr3_cnt", when(col("base_cvr3_cnt").isNull, 0).otherwise(col("base_cvr3_cnt")))
      .select("ideaid", "adclass", "cvr3_cnt", "base_cvr3_cnt")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))


    resultDF
  }

  def getK(date: String, hour: String, spark: SparkSession) = {
    val regressionK = getRegressionK(date, hour, spark)
    val pidK = getPIDk(date, hour, spark)
    val currentK = resetK(date, hour, regressionK, pidK, spark)
    val resultDF = currentK
      .select("ideaid", "adclass", "k_value2", "k_value3")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))


    resultDF
  }


  def getRegressionK(date: String, hour: String, spark: SparkSession) = {

    val sqlRequest =
      s"""
         |SELECT
         |  ideaid,
         |  k_ratio2,
         |  k_ratio3
         |FROM
         |  dl_cpc.ocpc_regression_k_final
         |WHERE
         |  `date` = '$date'
         |AND
         |  `hour` = '$hour'
       """.stripMargin

    val regressionK = spark.sql(sqlRequest)

    val resultDF = regressionK
      .select("ideaid", "k_ratio2", "k_ratio3")
      .withColumn("k_ratio2_regression", col("k_ratio2"))
      .withColumn("k_ratio3_regression", col("k_ratio3"))
      .select("ideaid", "k_ratio2_regression", "k_ratio3_regression")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    resultDF
  }

  def getPIDk(date: String, hour: String, spark: SparkSession) = {
    val resultDF = spark
      .table("dl_cpc.ocpc_k_value_table_hourly")
      .where(s"`date`='$date' and `hour`='$hour'")
      .select("ideaid", "adclass", "k_value2", "k_value3")
      .withColumn("k_ratio2_pid", col("k_value2"))
      .withColumn("k_ratio3_pid", col("k_value3"))
      .select("ideaid", "adclass", "k_ratio2_pid", "k_ratio3_pid")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    resultDF
  }

  def resetK(date: String, hour: String, regressionK: DataFrame, pidK: DataFrame, spark: SparkSession) = {
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


    val finalData = pidK
      .join(regressionK, Seq("ideaid"), "left_outer")
      .withColumn("new_k2", when(col("k_ratio2_regression")>0, col("k_ratio2_regression")).otherwise(col("k_ratio2_pid")))
      .withColumn("new_k3", when(col("k_ratio3_regression")>0, col("k_ratio3_regression")).otherwise(col("k_ratio3_pid")))
      .join(prevTable, Seq("ideaid", "adclass"), "left_outer")
      .withColumn("k_value2_middle", when(col("new_k2").isNotNull && col("prev_k2").isNotNull && col("new_k2")>col("prev_k2"), col("prev_k2") + (col("new_k2") - col("prev_k2")) * 1.0 / 4.0).otherwise(col("new_k2")))
      .withColumn("k_value3_middle", when(col("new_k3").isNotNull && col("prev_k3").isNotNull && col("new_k3")>col("prev_k3"), col("prev_k3") + (col("new_k3") - col("prev_k3")) * 1.0 / 4.0).otherwise(col("new_k3")))
      .withColumn("k_value2", when(col("flag")===0, col("prev_k2")).otherwise(col("k_value2_middle")))
      .withColumn("k_value3", when(col("flag")===0, col("prev_k3")).otherwise(col("k_value3_middle")))
      .select("ideaid", "adclass", "k_ratio2_regression", "k_ratio3_regression", "k_ratio2_pid", "k_ratio3_pid", "new_k2", "new_k3", "prev_k2", "prev_k3", "ctrcnt", "k_value2_middle", "k_value3_middle", "k_value2", "k_value3")


    finalData
  }

  def getPrevK(date: String, hour: String, hourCnt: Int, spark: SparkSession) = {
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

    val prevK = spark
      .table("dl_cpc.ocpc_pb_result_table_v7")
      .where(s"`date`='$date1' and `hour`='$hour1'")
      .withColumn("prev_k2", col("kvalue1"))
      .withColumn("prev_k3", col("kvalue2"))
      .select("ideaid", "adclass", "prev_k2", "prev_k3", "date", "hour")

    val prevCtr = spark
      .table("dl_cpc.ocpc_unionlog")
      .where(s"`dt`='$date1' and `hour`='$hour1'")
      .groupBy("ideaid", "adclass")
      .agg(sum(col("isclick")).alias("ctrcnt"))
      .select("ideaid", "adclass", "ctrcnt")

    val prevTable = prevK
      .join(prevCtr, Seq("ideaid", "adclass"), "left_outer")
      .select("ideaid", "adclass", "prev_k2", "prev_k3", "ctrcnt")
      .withColumn("flag", when(col("ctrcnt").isNotNull && col("ctrcnt")>0, 1).otherwise(0))
      .withColumn("date", lit(date1))
      .withColumn("hour", lit(hour1))

    prevTable
  }

//  baseData, hpcvr, apiCvr, resultK, date, hour, spark
  def assemblyPB(base: DataFrame, date: String, hour: String, spark: SparkSession) = {

    base.createOrReplaceTempView("base_table")

//    val ocpcIdeas = getIdeaUpdates(spark)
    val ocpcIdeas = spark.table("test.ocpc_idea_update_time_" + hour)
    ocpcIdeas.createOrReplaceTempView("ocpc_idea_update")
    val sqlRequest =
      s"""
         |SELECT
         |  a.ideaid,
         |  a.userid,
         |  a.adclass,
         |  a.cost,
         |  a.ctr_cnt,
         |  a.cvr_cnt,
         |  a.adclass_cost,
         |  a.adclass_ctr_cnt,
         |  a.adclass_cvr_cnt,
         |  a.hpcvr,
         |  1.0 as cali_value,
         |  1.0 as cvr3_cali,
         |  a.cvr3_cnt,
         |  (case when b.conversion_goal=1 and a.kvalue1>3.0 then 3.0
         |        when b.conversion_goal!=1 and a.kvalue1>2.0 then 2.0
         |        when b.conversion_goal is null and a.kvalue1>2.0 then 2.0
         |        when a.kvalue1<0 or a.kvalue1 is null then 0.0
         |        else a.kvalue1 end) as kvalue1,
         |  (case when a.kvalue2>2.0 then 2.0
         |        when a.kvalue2<0 or a.kvalue2 is null then 0.0
         |        else a.kvalue2 end) as kvalue2,
         |  a.is_ocpc_flag,
         |  b.conversion_goal
         |FROM
         |  base_table as a
         |LEFT JOIN
         |  ocpc_idea_update as b
         |ON
         |  a.ideaid=b.ideaid
       """.stripMargin
    println(sqlRequest)
    val result = spark
      .sql(sqlRequest)
      .na.fill(0.0, Seq("kvalue1", "kvalue2"))
      .withColumn("k_value", when(col("conversion_goal") === 2, col("kvalue2")).otherwise(col("kvalue1")))
      .filter(s"kvalue1 != 0 or kvalue2 != 0 or conversion_goal is not null")
      .filter("k_value > 0")


    val resultDF = result
      .selectExpr("ideaid", "userid", "adclass", "cost", "ctr_cnt", "cvr_cnt", "adclass_cost", "adclass_ctr_cnt", "adclass_cvr_cnt", "k_value", "hpcvr", "cast(cali_value as double) cali_value", "cast(cvr3_cali as double) cvr3_cali", "cvr3_cnt", "kvalue1", "kvalue2")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    resultDF
  }

  def initK(currentPb: DataFrame, date: String, hour: String, spark: SparkSession) = {
    // 对于刚进入ocpc阶段但是有cpc历史数据的广告依据历史转化率给出k的初值
    // 取历史数据
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val end_date = sdf.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(end_date)
    calendar.add(Calendar.DATE, -7)
    val dt = calendar.getTime
    val date1 = sdf.format(dt)
    val selectCondition = getTimeRangeSql3(date1, hour, date, hour)

    val ocpcHistoryData = spark
      .table("dl_cpc.ocpc_unionlog")
      .where(selectCondition)
      .select("ideaid", "adclass")
      .withColumn("is_ocpc_flag", lit(1))
      .distinct()

    val resultDF = currentPb
      .join(ocpcHistoryData, Seq("ideaid", "adclass"), "left_outer")
      .withColumn("kvalue1_middle", when(col("is_ocpc_flag").isNull, col("cvr_cnt") * 1.0 / (col("ctr_cnt") * col("hpcvr"))).otherwise(col("kvalue1_init")))
      .withColumn("kvalue2_middle", when(col("is_ocpc_flag").isNull, col("cvr3_cnt") * 1.0 / (col("ctr_cnt") * col("hpcvr"))).otherwise(col("kvalue2_init")))
      .withColumn("kvalue1", when(col("kvalue1_middle").isNull, 0.0).otherwise(col("kvalue1_middle")))
      .withColumn("kvalue2", when(col("kvalue2_middle").isNull, 0.0).otherwise(col("kvalue2_middle")))



    resultDF
  }

  def initKv2(currentPb: DataFrame, date: String, hour: String, spark: SparkSession) = {
    /*
    通过slim_union_log关联的方式获取前72小时中的k值
    1. 以searchid关联的方式关联k值与cvr
    2. 计算各个ideaid的实际cvr
    3. 按照实际cvr的2倍过滤过高cvr
     */
    // 对于刚进入ocpc阶段但是有cpc历史数据的广告依据历史转化率给出k的初值
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
    val selectCondition2 = getTimeRangeSql3(date2, hour, date, hour)

    println(selectCondition2)
    val ocpcHistoryData = spark
      .table("dl_cpc.ocpc_unionlog")
      .where(selectCondition2)
      .select("ideaid", "adclass")
      .withColumn("is_ocpc_flag", lit(1))
      .distinct()

    // conversiongoal=1 or 3
    val sqlRequest1 =
      s"""
         |SELECT
         |    a.searchid,
         |    a.ideaid,
         |    a.adclass,
         |    a.exp_cvr,
         |    a.isclick,
         |    b.iscvr
         |FROM
         |    (SELECT
         |        searchid,
         |        ideaid,
         |        adclass,
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
         |        1 as iscvr
         |    FROM
         |        dl_cpc.ml_cvr_feature_v1
         |    WHERE
         |        `date`>='$date1'
         |    AND
         |        label_type in (1, 2, 3, 4, 5, 6)
         |    AND
         |        label2=1
         |    GROUP BY searchid) as b
         |ON
         |    a.searchid=b.searchid
       """.stripMargin
    println(sqlRequest1)
    val data1 = spark.sql(sqlRequest1)
    val cvrData1 = data1
        .na.fill(0, Seq("iscvr"))
        .groupBy("ideaid", "adclass")
        .agg(
          sum(col("isclick")).alias("click"),
          sum(col("iscvr")).alias("conversion")
        )
        .withColumn("post_cvr", col("conversion") * 1.0 / col("click"))
        .withColumn("post_cvr_cali", col("post_cvr") * 5.0)
        .select("ideaid", "adclass", "post_cvr", "post_cvr_cali")

    val caliData1 = data1
        .join(cvrData1, Seq("ideaid", "adclass"), "left_outer")
        .select("searchid", "ideaid", "adclass", "exp_cvr", "isclick", "iscvr", "post_cvr", "post_cvr_cali")
        .withColumn("pre_cvr", when(col("exp_cvr")> col("post_cvr_cali"), col("post_cvr_cali")).otherwise(col("exp_cvr")))
        .select("searchid", "ideaid", "adclass", "exp_cvr", "isclick", "iscvr", "post_cvr", "pre_cvr", "post_cvr_cali")

    val finalData1 = caliData1
        .groupBy("ideaid", "adclass")
        .agg(
          sum(col("pre_cvr")).alias("pre_cvr"),
          sum(col("isclick")).alias("click")
        )
        .withColumn("pre_cvr", col("pre_cvr") * 1.0 / col("click"))
        .select("ideaid", "adclass", "pre_cvr")
        .join(cvrData1, Seq("ideaid", "adclass"), "left_outer")
        .withColumn("kvalue1_middle", col("post_cvr") * 1.0 / col("pre_cvr"))
        .select("ideaid", "adclass", "kvalue1_middle")

    // conversiongoal=2
    val sqlRequest2 =
      s"""
         |SELECT
         |    a.searchid,
         |    a.ideaid,
         |    a.adclass,
         |    a.exp_cvr,
         |    a.isclick,
         |    b.iscvr
         |FROM
         |    (SELECT
         |        searchid,
         |        ideaid,
         |        adclass,
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
         |        1 as iscvr
         |    FROM
         |        dl_cpc.ml_cvr_feature_v2
         |    WHERE
         |        `date`>='$date1'
         |    AND
         |        label=1
         |    GROUP BY searchid) as b
         |ON
         |    a.searchid=b.searchid
       """.stripMargin
    println(sqlRequest2)
    val data2 = spark.sql(sqlRequest2)
    val cvrData2 = data2
        .na.fill(0, Seq("iscvr"))
        .groupBy("ideaid", "adclass")
        .agg(
          sum(col("isclick")).alias("click"),
          sum(col("iscvr")).alias("conversion")
        )
        .withColumn("post_cvr", col("conversion") * 1.0 / col("click"))
        .withColumn("post_cvr_cali", col("post_cvr") * 5.0)
        .select("ideaid", "adclass", "post_cvr", "post_cvr_cali")

    val caliData2 = data2
        .join(cvrData2, Seq("ideaid", "adclass"), "left_outer")
        .select("searchid", "ideaid", "adclass", "exp_cvr", "isclick", "iscvr", "post_cvr", "post_cvr_cali")
        .withColumn("pre_cvr", when(col("exp_cvr")>col("post_cvr_cali"), col("post_cvr_cali")).otherwise(col("exp_cvr")))
        .select("searchid", "ideaid", "adclass", "exp_cvr", "isclick", "iscvr", "post_cvr", "pre_cvr", "post_cvr_cali")

    val finalData2 = caliData2
        .groupBy("ideaid", "adclass")
        .agg(
          sum(col("pre_cvr")).alias("pre_cvr"),
          sum(col("isclick")).alias("click")
        )
        .withColumn("pre_cvr", col("pre_cvr") * 1.0 / col("click"))
        .select("ideaid", "adclass", "pre_cvr")
        .join(cvrData2, Seq("ideaid", "adclass"), "left_outer")
        .withColumn("kvalue2_middle", col("post_cvr") * 1.0 / col("pre_cvr"))
        .select("ideaid", "adclass", "kvalue2_middle")

    val finalData = finalData1
        .join(finalData2, Seq("ideaid", "adclass"), "outer")
        .select("ideaid", "adclass", "kvalue1_middle", "kvalue2_middle")

    // 关联currentPb和ocpc_flag
    val resultDF = currentPb
        .join(ocpcHistoryData, Seq("ideaid", "adclass"), "left_outer")
        .join(finalData, Seq("ideaid", "adclass"), "left_outer")
      .withColumn("kvalue1_middle", when(col("is_ocpc_flag").isNull, col("kvalue1_middle")).otherwise(col("kvalue1_init")))
      .withColumn("kvalue2_middle", when(col("is_ocpc_flag").isNull, col("kvalue2_middle")).otherwise(col("kvalue2_init")))
      .withColumn("kvalue1", when(col("kvalue1_middle").isNull, 0.0).otherwise(col("kvalue1_middle")))
      .withColumn("kvalue2", when(col("kvalue2_middle").isNull, 0.0).otherwise(col("kvalue2_middle")))

    resultDF
  }

}

