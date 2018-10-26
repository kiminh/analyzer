package com.cpc.spark.ocpc

import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import com.cpc.spark.udfs.Udfs_wj._
import org.apache.spark.sql.functions._


object OcpcPIDwithCPA {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("OcpcPIDwithCPA").enableHiveSupport().getOrCreate()

    val date = args(0).toString
    val hour = args(1).toString
    val isTest = args(2).toInt

    if (isTest == 1) {
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
    } else {
      println("############## entering test stage ###################")
      // 初始化K值
      testCalculateK(date, hour, spark)
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
    val selectCondition1 = s"`date`='$date1' and `hour` >= '$hour1'"
    val selectCondition2 = s"`date`='$date' and `hour`<='$hour'"

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
         |    ($selectCondition1) OR ($selectCondition2)
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

//    resultDF.show(10)
//
//    resultDF.filter("history_k is not null").show(10)
//
//    resultDF.filter("flag=0").show(10)

    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_k_value_percent_flag")

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
    val selectCondition1 = s"`date`='$date1' and hour >= '$hour1'"
    val selectCondition2 = s"`date`='$date' and `hour`<='$hour'"

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
         |        ($selectCondition1)
         |    OR
         |        ($selectCondition2)
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
         |  (SELECT * FROM dl_cpc.ocpc_pb_result_table WHERE `date`='$date' and `hour`='$hour') as a
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
         |  test.new_pb_ocpc_with_pcvr as a
         |LEFT JOIN
         |  test.ocpc_cpa_given_history_ratio as b
         |ON
         |  a.ideaid=b.ideaid
         |LEFT JOIN
         |  test.ocpc_k_value_percent_flag as c
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

}
