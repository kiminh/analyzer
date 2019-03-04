package com.cpc.spark.ocpcV3.HotTopicOcpc.model

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.{ Column,    Dataset, Row, SparkSession }
import org.apache.spark.sql.functions._

import com.cpc.spark.ocpc.OcpcUtils._
import com.cpc.spark.udfs.Udfs_wj._

import scala.collection.mutable

object OcpcPIDwithCPA {
  /*
    用于ocpc明投的相关代码
   */
  def main(args: Array[String]): Unit = {
    /*
    根据PID控制调整k值：
    1. 获得历史k值
    2. 获得历史cpa
    3. 根据给定cpa计算cpa_ratio
    4. 更新k值
     */
    val spark = SparkSession.builder().appName("OcpcPIDwithCPA").enableHiveSupport().getOrCreate()

    // bash: 2019-01-02 12 version2 novel
    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val media = args(3).toString
    var mediaSelection = s"media_appsid in ('80000001', '80000002')"
    if (media == "qtt") {
      mediaSelection = s"media_appsid in ('80000001', '80000002')"
    } else if(media == "novel"){
      mediaSelection = s"media_appsid in ('80001098','80001292')"
    } else {
      mediaSelection = s"media_appsid = '80002819'"
    }

    val prevTable = spark                           //prevTable中的数据为identifier(unitid)对应的cpa_given, k值, 转化数
      .table("test.ocpc_hottopic_prev_pb_hourly" )
      .where(s"version='$version'" )
    val cvrData = getCVR1data( date, hour, spark )  //返回dl_cpc.ml_cvr_feature_v1中在date日，hour时之前24h内label_type!=12且有转化的search_id

    val historyData = getHistory(mediaSelection, date, hour, spark) // 获取ocpc_union_log_hourly中date日，hour时之前24h内,热点段子的每个searchid所对应的广告信息，浏览点击，k值，cpa等相关数据
    val kvalue      = getHistoryK( historyData, prevTable, date, hour, spark ) // 返回每个unitid（identifier）对应的最终kvalue
    val cpaHistory  = getCPAhistory( historyData, cvrData, 1, date, hour, spark ) //返回identifier, cpa（实际计算出来的，cost/cvrcnt）, cvrcnt, cost, avg(cpagiven) as cpagiven, conversion_goal
    val cpaRatio    = calculateCPAratio(cpaHistory, date, hour, spark) // identifier, cpagiven, cost, cvrcnt, cpa, cpa_ratio, conversion_goal
    val result      = updateK( kvalue, cpaRatio, date, hour, spark )   //
    val resultDF    = result
      .select("identifier", "k_value", "conversion_goal" )
      .withColumn("date",    lit(date)    )
      .withColumn("hour",    lit(hour)    )
      .withColumn("version", lit(version) )

//    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_pid_k_hourly0304")
    resultDF.repartition(10)
      .write
      .mode("overwrite" )
      .insertInto("dl_cpc.ocpc_pid_k_hourly" )
  }

  def getHistory(mediaSelection: String, date: String, hour: String, spark: SparkSession) = {
    /**
      * 取历史数据
      * 获取ocpc_union_log_hourly中date日，hour时之前24h内searchid, unitid, cast(unitid as string) identifier, adclass, isshow, isclick, price, ocpc_log, ocpc_log_dict,
      *  ocpc_log_dict['kvalue'] as kvalue, cpagiven, hour
      */
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -24)
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
         |  unitid,
         |  cast(unitid as string) identifier,
         |  ext['adclass'].int_value as adclass,
         |  isshow,
         |  isclick,
         |  price,
         |  ocpc_log,
         |  ocpc_log_dict,
         |  ocpc_log_dict['kvalue']   as kvalue,
         |  ocpc_log_dict['cpagiven'] as cpagiven,  --目标cpa
         |  hour
         |FROM
         |  dl_cpc.ocpc_union_log_hourly
         |WHERE
         |  $selectCondition
         |AND
         |  $mediaSelection
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)
    resultDF
  }

  def getCVR1data(date: String, hour: String, spark: SparkSession) = {
    /**
      * 根据需要调整获取cvr的函数
      * 返回dl_cpc.ml_cvr_feature_v1中在date日，hour时之前24h内label_type!=12且有转化的search_id
      */
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -24)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)

    val resultDF = spark
      .table("dl_cpc.ml_cvr_feature_v1" )
      .where( selectCondition )
      .filter(s"label_type!=12" ) //ocpc不做应用商城的广告
      .withColumn("iscvr", col("label2"))
      .select("searchid", "iscvr")
      .filter("iscvr=1")
      .distinct()
    resultDF
  }

  def getHistoryK(historyData: DataFrame, prevPb: DataFrame, date: String, hour: String, spark: SparkSession) = {
    /**
      * 计算修正前的k基准值
      * case1：前6个小时有  isclick=1的数据，统计这批数据的k均值作为基准值
      * case2：前6个小时没有isclick=1的数据，将前一个小时的数据作为基准值
      * 返回每个unitid（identifier）对应的最终kvalue
      */
    // case1
    val case1 = historyData
      .filter("isclick=1" )
      .groupBy("identifier" )
      .agg( avg(col("kvalue")).alias("kvalue1" ) )
      .select("identifier", "kvalue1" )

    // case2
    val case2 = prevPb
      .withColumn("kvalue2", col("kvalue"))
      .select("identifier", "kvalue2")
      .distinct()

    // 优先case1，然后case2，最后case3
    val resultDF = case1
      .join(case2, Seq("identifier"), "outer")
      .select("identifier", "kvalue1", "kvalue2")
      .withColumn("kvalue", when(col("kvalue1").isNull, col("kvalue2")).otherwise(col("kvalue1")))
    resultDF
  }

  def getCPAhistory(historyData: DataFrame, cvrRaw: DataFrame, conversionGoal: Int, date: String, hour: String, spark: SparkSession) = {
    /**
      * 计算cpa_history，分为cvr2和cvr3
      * 1. 获取cost
      * 2. 获取cvr
      * 3. 计算cpa_history
      *  返回identifier, cpa, cvrcnt, cost, avg(cpagiven) as cpagiven, conversion_goal
      */
    // cost data
    val costData = historyData
      .filter("isclick=1")
      .groupBy("identifier")
      .agg(
        sum(col("price"   )).alias("cost"    ),
        avg(col("cpagiven")).alias("cpagiven") )
      .select("identifier", "cost", "cpagiven" )

    // cvr data
    // 用searchid关联
    val cvrData = historyData
      .join( cvrRaw, Seq("searchid"), "left_outer" )
      .groupBy("identifier")
      .agg(sum(col("iscvr")).alias("cvrcnt"))
      .select("identifier", "cvrcnt")

    // 计算cpa
    val resultDF = costData
      .join( cvrData, Seq("identifier"), "left_outer" )
      .withColumn("cpa", col("cost") * 1.0/col("cvrcnt") )
      .withColumn("conversion_goal", lit(conversionGoal) )
      .select("identifier", "cpa", "cvrcnt", "cost", "cpagiven", "conversion_goal")
    resultDF
  }

  def calculateCPAratio(cpaHistory: DataFrame, date: String, hour: String, spark: SparkSession) = {
    cpaHistory.createOrReplaceTempView("raw_table")

    val sqlRequest =
      s"""
         |SELECT
         |  identifier,
         |  cpagiven,
         |  cost,
         |  cvrcnt,
         |  cpa,
         |  (case when cpagiven is null then 1.0
         |        when cvrcnt is null or cvrcnt = 0 then 0.8
         |        when cvrcnt>0 then cpagiven * 1.0 / cpa
         |        else 1.0 end ) as cpa_ratio,
         |   conversion_goal
         |FROM
         |  raw_table
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)
    resultDF

  }

  def updateK( kvalue: DataFrame, cpaRatio: DataFrame, date: String, hour: String, spark: SparkSession ) = {
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
    val rawData = kvalue
      .join(cpaRatio, Seq("identifier"), "outer")
      .select("identifier", "cpa_ratio", "conversion_goal", "kvalue")

    val unitAdclassMap = getAdclassMap(date, hour, spark)

    val resultDF = rawData
        .withColumn("adclassInt", getAdclass(unitAdclassMap)(col("identifier")))
        .withColumn("ratio_tag", udfSetRatioCase()(col("cpa_ratio")) )
        .withColumn("updated_k", udfUpdateK2()(col("ratio_tag"), col("kvalue"), col("adclassInt")) )
        .withColumn("k_value",   col("updated_k") )

    resultDF

  }

  def getAdclassMap( date: String, hour: String, spark: SparkSession) = {
    // 取历史数据
    /***
      * 返回ocpc_union_log_hourly中date日，hour时之前hourCnt小时内的searchid,unitid, identifier,isclick, price, cpagiven,kvalue
      */
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse( newDate )
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, 72)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)

    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  adclass,
         |  sum(ctr_cnt) as click
         |FROM
         |  dl_cpc.ocpc_ctr_data_hourly
         |WHERE
         |  $selectCondition
         |AND
         |  media_appsid in ("80000001", "80000002", "80002819")
         |GROUP BY unitid, adclass
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)
    data.createOrReplaceTempView("base_data")

    val sqlRequest1 =
      s"""
         |SELECT
         |  t.unitid,
         |  t.adclass,
         |  t.click,
         |  t.seq
         |FROM
         |  (SELECT
         |      unitid,
         |      adclass,
         |      click,
         |      row_number() over(partition by unitid order by click desc) as seq
         |   FROM
         |       base_data) as t
         |WHERE
         |  t.seq=1
       """.stripMargin
    println(sqlRequest1)
    val unitidAdclass = spark.sql(sqlRequest1)

    unitidAdclass.show(10)
    unitidAdclass.write.mode("overwrite").saveAsTable("test.sjq_unit_adclass_map")

    var adclassMap = mutable.LinkedHashMap[String, Int]()
    for(row <- unitidAdclass.collect()) {
      val unitid = row.getAs[Int]("unitid").toString
      val adclass = row.getAs[Int]("adclass")
      adclassMap += (unitid -> adclass)
    }
    adclassMap

  }

  def getAdclass(unit_adclass_map: mutable.LinkedHashMap[String, Int]) = udf( (identifier: String ) => {
    val adclass = unit_adclass_map.getOrElse(identifier, 0)/1000
    val adclassInt = adclass.toInt
    adclassInt
  })

  def udfUpdateK2() = udf(( valueTag: Int, valueK: Double, adclassInt: Int ) => {
    /**
      * 根据新的K基准值和cpa_ratio来在分段函数中重新定义k值
      * t1: k * 1.2 or k
      * t2: k / 1.6
      * t3: k / 1.4
      * t4: k / 1.2
      * t5: k / 1.1
      * t6: k
      * t7: k * 1.05
      * t8: k * 1.1
      * t9: k * 1.2
      * t10: k * 1.3
      *
      * 上下限依然是0.2 到1.2
      */
    val result = valueTag match {
      case 1 if valueK >= 1.2 => valueK
      case 1 if valueK < 1.2 => valueK * 1.1
      case 2 => valueK / 2.5
      case 3 => valueK / 2.0
      case 4 => valueK / 1.8
      case 5 => valueK / 1.5
      case 6 => valueK
      case 7 => valueK * 1.1
      case 8 => valueK * 1.2
      case 9 => valueK * 1.4
      case 10 => valueK * 1.6
      case _ => valueK
    }
    if(adclassInt == 110110){
      0.6163*result
    }else{
      result
    }
  })

}
