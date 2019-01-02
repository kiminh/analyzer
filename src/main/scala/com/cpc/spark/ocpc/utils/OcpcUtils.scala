package com.cpc.spark.ocpc.utils

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.common.Utils.getTimeRangeSql
import com.cpc.spark.ocpc.OcpcSampleToRedis.checkAdType
import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import userocpc.userocpc.{SingleUser, UserOcpc}

import scala.collection.mutable.ListBuffer

object OcpcUtils {
  def filterDataByType(rawData: DataFrame, date:String, hour: String, spark:SparkSession): DataFrame ={
    /**
      * 如果表单类广告数量过少，使用行业类别数据来替代
      */
    // TODO 使广告行业的ctr与cvr的替换更加平滑
    // TODO 还需不需要这个阶段？
    val typeData = spark
      .table("test.ocpc_idea_update_time_" + hour)
      .withColumn("type_flag", when(col("conversion_goal")===3, 1).otherwise(0)).select("ideaid", "type_flag")

    val joinData = rawData
      .join(typeData, Seq("ideaid"), "left_outer")
      .select("ideaid", "userid", "adclass", "cost", "ctr_cnt", "cvr_cnt", "adclass_cost", "adclass_ctr_cnt", "adclass_cvr_cnt", "type_flag")
      .withColumn("new_type_flag", when(col("type_flag").isNull, 0).otherwise(col("type_flag")))

    joinData.createOrReplaceTempView("join_table")

    val sqlRequest1 =
      s"""
         |SELECT
         |    adclass,
         |    new_type_flag,
         |    SUM(cost) as total_cost,
         |    SUM(ctr_cnt) as total_ctr,
         |    SUM(cvr_cnt) as total_cvr
         |FROM
         |    join_table
         |GROUP BY adclass, new_type_flag
       """.stripMargin

    println(sqlRequest1)
    val groupbyData = spark.sql(sqlRequest1)

    val joinData2 = joinData
      .join(groupbyData, Seq("adclass", "new_type_flag"), "left_outer")
      .select("ideaid", "userid", "adclass", "cost", "ctr_cnt", "cvr_cnt", "adclass_cost", "adclass_ctr_cnt", "adclass_cvr_cnt", "new_type_flag", "total_cost", "total_ctr", "total_cvr")
      .withColumn("new_cost", when(col("cvr_cnt")<20 && col("new_type_flag")===1, col("total_cost")).otherwise(col("cost")))
      .withColumn("new_ctr_cnt", when(col("cvr_cnt")<20 && col("new_type_flag")===1, col("total_ctr")).otherwise(col("ctr_cnt")))
      .withColumn("new_cvr_cnt", when(col("cvr_cnt")<20 && col("new_type_flag")===1, col("total_cvr")).otherwise(col("cvr_cnt")))


    joinData2.write.mode("overwrite").saveAsTable("test.ocpc_final_join_table")
    joinData2
  }

  def getCvr3Data(date: String, hour: String, hourCnt: Int, spark: SparkSession) :DataFrame ={
    /**
      * 按照给定的时间区间获取从OcpcActivationData程序的结果表获取历史数据
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

    // read data and set redis configuration
    val sqlRequest =
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
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)
    resultDF
  }

  def calculateHPCVR(endDate: String, hour: String, spark: SparkSession) ={
    // calculate time period for historical data
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val date = sdf.parse(endDate)
    val calendar = Calendar.getInstance
    calendar.setTime(date)
    calendar.add(Calendar.DATE, -7)
    val dt = calendar.getTime
    val startDate = sdf.format(dt)
    val selectCondition = getTimeRangeSql(startDate, hour, endDate, hour)

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

    val rawTable = spark.sql(sqlRequest)

    rawTable.write.mode("overwrite").saveAsTable("test.ocpc_hpcvr_total")
    rawTable

  }

  def getRegressionK(date: String, hour: String, spark: SparkSession) = {
    import spark.implicits._

    val filename = "/user/cpc/wangjun/ocpc_linearregression_k.txt"
    val data = spark.sparkContext.textFile(filename)


    val rawRDD = data.map(x => (x.split(",")(0).toInt, x.split(",")(1).toInt))
    rawRDD.foreach(println)

    val rawDF = rawRDD.toDF("ideaid", "flag").distinct()

    val sqlRequest =
      s"""
         |SELECT
         |  ideaid,
         |  k_ratio2,
         |  k_ratio3
         |FROM
         |  dl_cpc.ocpc_v2_k
         |WHERE
         |  `date` = '$date'
         |AND
         |  `hour` = '$hour'
       """.stripMargin

    val regressionK = spark.sql(sqlRequest)

    val cvr3List = spark
      .table("test.ocpc_idea_update_time_" + hour)
      .filter("conversion_goal=2")
      .withColumn("cvr3_flag", lit(1))
      .select("ideaid", "cvr3_flag")
      .distinct()

    val resultDF = rawDF
      .join(regressionK, Seq("ideaid"), "left_outer")
      .select("ideaid", "k_ratio2", "k_ratio3", "flag")
      .join(cvr3List, Seq("ideaid"), "left_outer")
      .select("ideaid", "flag", "k_ratio2", "k_ratio3", "cvr3_flag")
      .withColumn("regression_k_value", when(col("cvr3_flag").isNull, col("k_ratio2")).otherwise(col("k_ratio3")))

    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_test_k_regression_list")

    resultDF


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

      if (cnt % 500 == 0) {
        println(s"ideaid:$ideaid, userId:$userId, adclassId:$adclassId, costValue:$costValue, ctrValue:$ctrValue, cvrValue:$cvrValue, adclassCost:$adclassCost, adclassCtr:$adclassCtr, adclassCvr:$adclassCvr, k:$k, hpcvr:$hpcvr, caliValue:$caliValue, cvr3Cali:$cvr3Cali, cvr3Cnt:$cvr3Cnt")
      }
      cnt += 1
      //      if (cvr3Cnt > 0) {
      //        println("######################")
      //        println(s"ideaid:$ideaid, userId:$userId, adclassId:$adclassId, costValue:$costValue, ctrValue:$ctrValue, cvrValue:$cvrValue, adclassCost:$adclassCost, adclassCtr:$adclassCtr, adclassCvr:$adclassCvr, k:$k, hpcvr:$hpcvr, caliValue:$caliValue, cvr3Cali:$cvr3Cali, cvr3Cnt:$cvr3Cnt")
      //      }

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
          cvr3Cnt = cvr3Cnt
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

  def getTimeRangeSql2(startDate: String, startHour: String, endDate: String, endHour: String): String = {
    if (startDate.equals(endDate)) {
      return s"(`date` = '$startDate' and hour <= '$endHour' and hour > '$startHour')"
    }
    return s"((`date` = '$startDate' and hour > '$startHour') " +
      s"or (`date` = '$endDate' and hour <= '$endHour') " +
      s"or (`date` > '$startDate' and `date` < '$endDate'))"
  }

  def getTimeRangeSql3(startDate: String, startHour: String, endDate: String, endHour: String): String = {
    if (startDate.equals(endDate)) {
      return s"(`dt` = '$startDate' and hour <= '$endHour' and hour > '$startHour')"
    }
    return s"((`dt` = '$startDate' and hour > '$startHour') " +
      s"or (`dt` = '$endDate' and hour <= '$endHour') " +
      s"or (`dt` > '$startDate' and `dt` < '$endDate'))"
  }

  def getIdeaUpdates(spark: SparkSession) = {
    val url = "jdbc:mysql://rr-2zehhy0xn8833n2u5.mysql.rds.aliyuncs.com:3306/adv?useUnicode=true&characterEncoding=utf-8"
    val user = "adv_live_read"
    val passwd = "seJzIPUc7xU"
    val driver = "com.mysql.jdbc.Driver"
    val table = "(select ideas, bid, ocpc_bid, ocpc_bid_update_time, cast(conversion_goal as char) as conversion_goal from adv.unit where is_ocpc=1 and ideas is not null) as tmp"


    val data = spark.read.format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("user", user)
      .option("password", passwd)
      .option("dbtable", table)
      .load()

    val base = data.select("ideas", "bid", "ocpc_bid", "ocpc_bid_update_time", "conversion_goal")


    val ideaTable = base.withColumn("ideaid", explode(split(col("ideas"), "[,]"))).select("ideaid", "ocpc_bid", "ocpc_bid_update_time", "conversion_goal")

    ideaTable.createOrReplaceTempView("ideaid_update_time")

    val sqlRequest =
      s"""
         |SELECT
         |    t.ideaid,
         |    t.ocpc_bid as cpa_given,
         |    t.conversion_goal,
         |    from_unixtime(t.ocpc_bid_update_time) as update_time,
         |    from_unixtime(t.ocpc_bid_update_time, 'yyyy-MM-dd') as update_date,
         |    from_unixtime(t.ocpc_bid_update_time, 'HH') as update_hour
         |
         |FROM
         |    (SELECT
         |        ideaid,
         |        ocpc_bid,
         |        ocpc_bid_update_time,
         |        cast(conversion_goal as int) as conversion_goal,
         |        row_number() over(partition by ideaid order by ocpc_bid_update_time desc) as seq
         |    FROM
         |        ideaid_update_time) t
         |WHERE
         |    t.seq=1
       """.stripMargin

    println(sqlRequest)

    val updateTime = spark.sql(sqlRequest)

    println("########## updateTime #################")
    updateTime.show(10)
    updateTime
  }

}
