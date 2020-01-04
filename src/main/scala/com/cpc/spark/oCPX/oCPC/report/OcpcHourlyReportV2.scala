package com.cpc.spark.oCPX.oCPC.report

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools._
import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcHourlyReportV2 {
  def main(args: Array[String]): Unit = {
    /*
    新版报表程序
    1. 从ocpc_unionlog拉取ocpc广告记录
    2. 采用数据关联方式获取转化数据
    3. 统计分ideaid级别相关数据
    4. 统计分conversion_goal级别相关数据
    5. 存储到hdfs
    6. 存储到mysql
     */
    val spark = SparkSession
      .builder()
      .appName("OcpcHourlyReport")
      .enableHiveSupport()
      .getOrCreate()

    val date = args(0).toString
    val dayInt = args(1).toInt
    val dbName = args(2).toString
    println("parameters:")
    println(s"date=$date, dayInt=$dayInt, dbName=$dbName")

    // 拉取点击、消费、转化等基础数据
    val rawData = getBaseData(date, dayInt, spark)

    // stage3
    val stage3DataRaw = rawData.filter(s"deep_ocpc_step = 2")
    val stage3Data = calculateData(stage3DataRaw, 3, spark)

    // stage2
    val stage2DataRaw = rawData.filter(s"deep_ocpc_step != 2 and ocpc_step = 2")
    val stage2Data = calculateData(stage2DataRaw, 2, spark)

    // stage1
    val stage1DataRaw = rawData.filter(s"ocpc_step = 1")
    val stage1Data = calculateData(stage1DataRaw, 1, spark)

    // data union
    val data = stage1Data.union(stage2Data).union(stage3Data)

    // get ad name
    val adProdName = getAdProdName(spark)

    saveDataToHDFS(data, adProdName, dbName, spark)

  }

  def getAdProdName(spark: SparkSession) = {
    val conf = ConfigFactory.load("ocpc")

    val url = conf.getString("adv_read_mysql.cpc_sales_crm.url")
    val user = conf.getString("adv_read_mysql.cpc_sales_crm.user")
    val passwd = conf.getString("adv_read_mysql.cpc_sales_crm.password")
    val driver = conf.getString("adv_read_mysql.cpc_sales_crm.driver")
    val table =
      s"""
         |(SELECT a.adv_user_id , p.name
         |FROM customer_adv_account a
         |left join product p
         |on a.product_id = p.id
         |GROUP BY a.adv_user_id , p.name) as tmp
         |""".stripMargin

    val data = spark.read.format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("user", user)
      .option("password", passwd)
      .option("dbtable", table)
      .load()

    val resultDF = data
      .withColumn("userid", col("adv_user_id"))
      .withColumn("prod_name", col("name"))
      .selectExpr("userid", "prod_name")
      .filter(s"prod_name is not null")
      .distinct()

    resultDF.show(10)
    resultDF
  }

  def saveDataToHDFS(data: DataFrame, adProdName: DataFrame, dbName: String, spark: SparkSession) = {
    val result = data
      .join(adProdName, Seq("userid"), "left_outer")
      .na.fill(" ", Seq("prod_name"))
      .select("ideaid", "unitid", "userid", "prod_name", "adclass", "adslot_type", "adslotid", "conversion_goal", "deep_conversion_goal", "cpa_check_priority", "media_appsid", "ocpc_expand", "show", "click", "cv1", "cv2", "total_price", "total_bid", "total_precvr", "total_rawcvr", "total_prectr", "total_exp_cpm", "total_cpagiven", "total_jfbfactor", "total_cvrfactor", "total_calipcvr", "total_discrete_factor", "total_shallow_bid", "bl_hidden_tax", "bk_hidden_tax", "total_deep_cpagiven", "total_deep_jfbfactor", "total_deep_cvrfactor", "total_deep_calipcvr", "total_deep_bid", "total_deepcvr", "ocpc_stage", "date", "hour")

    val tableName = s"$dbName.ocpc_report_data_hourly"
    println(s"save data to $tableName")
    result
      .repartition(1)
      .write.mode("overwrite").insertInto(tableName)
  }

  def calculateData(rawData: DataFrame, ocpcStage: Int, spark: SparkSession) = {
    rawData.createOrReplaceTempView("raw_data")
    val sqlRequest =
      s"""
         |SELECT
         |  ideaid,
         |  unitid,
         |  userid,
         |  adclass,
         |  adslot_type,
         |  adslotid,
         |  conversion_goal,
         |  deep_conversion_goal,
         |  cpa_check_priority,
         |  media_appsid,
         |  ocpc_expand,
         |  date,
         |  hour,
         |  sum(isshow) as show,
         |  sum(isclick) as click,
         |  sum(case when isclick=1 then iscvr1 else 0 end) as cv1,
         |  sum(case when isclick=1 then iscvr2 else 0 end) as cv2,
         |  sum(case when isclick=1 then price else 0 end) as total_price,
         |  sum(case when isclick=1 then bid else 0 end) as total_bid,
         |  sum(case when isclick=1 then exp_cvr else 0 end) * 1.0 as total_precvr,
         |  sum(case when isclick=1 then raw_cvr else 0 end) * 1.0 as total_rawcvr,
         |  sum(case when isshow=1 then exp_ctr else 0 end) * 1.0 as total_prectr,
         |  sum(case when isshow=1 then exp_cpm else 0 end) * 1.0 as total_exp_cpm,
         |  sum(case when isclick=1 then cast(ocpc_log_dict['cpagiven'] as double) else 0 end) as total_cpagiven,
         |  sum(case when isclick=1 then cast(ocpc_log_dict['kvalue'] as double) else 0 end) * 1.0 as total_jfbfactor,
         |  sum(case when isclick=1 then cast(ocpc_log_dict['cvrCalFactor'] as double) else 0 end) * 1.0 as total_cvrfactor,
         |  sum(case when isclick=1 then cast(ocpc_log_dict['pcvr'] as double) else 0 end) * 1.0 as total_calipcvr,
         |  sum(case when isclick=1 then cast(ocpc_log_dict['discreteFactor'] as double) else 0 end) as total_discrete_factor,
         |  sum(case when isclick=1 then cast(ocpc_log_dict['dynamicbid'] as double) else 0 end) * 1.0 as total_shallow_bid,
         |  sum(case when isclick=1 and hidden_tax < 0 then -hidden_tax else 0 end) as bl_hidden_tax,
         |  sum(case when isclick=1 and hidden_tax > 0 then hidden_tax else 0 end) as bk_hidden_tax,
         |  sum(case when isclick=1 then cast(deep_cpa as double) else 0 end) as total_deep_cpagiven,
         |  sum(case when isclick=1 then cast(deep_ocpc_log_dict['kvalue'] as double) else 0 end) * 1.0 as total_deep_jfbfactor,
         |  sum(case when isclick=1 then cast(deep_ocpc_log_dict['cvrCalFactor'] as double) else 0 end) * 1.0 as total_deep_cvrfactor,
         |  sum(case when isclick=1 then cast(deep_ocpc_log_dict['pcvr'] as double) else 0 end) * 1.0 as total_deep_calipcvr,
         |  sum(case when isclick=1 then cast(deep_ocpc_log_dict['dynamicbid'] as double) else 0 end) * 1.0 as total_deep_bid,
         |  sum(case when isclick=1 then exp_cvr2 else 0 end) * 1.0 as total_deepcvr
         |FROM
         |  raw_data
         |GROUP BY ideaid, unitid, userid, adclass, adslot_type, adslotid, conversion_goal, deep_conversion_goal, cpa_check_priority, media_appsid, ocpc_expand, date, hour
       """.stripMargin
    println(sqlRequest)
    val data = spark
      .sql(sqlRequest)
      .withColumn("ocpc_stage", lit(ocpcStage))
      .cache()
    println("base_data:")
    data.show(10)
    data
  }

  def getBaseData(date: String, dayInt: Int, spark: SparkSession) = {
    /**
      * 重新计算抽取全天截止当前时间的数据日志
      */

    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -dayInt)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)
    // 抽取基础数据：所有跑ocpc的广告主
    val sqlRequest1 =
      s"""
         |SELECT
         |    searchid,
         |    ideaid,
         |    unitid,
         |    userid,
         |    adslot_type,
         |    adslotid,
         |    adclass,
         |    conversion_goal,
         |    conversion_from,
         |    deep_conversion_goal,
         |    cpa_check_priority,
         |    is_deep_ocpc,
         |    ocpc_expand,
         |    isclick,
         |    isshow,
         |    price,
         |    bid_discounted_by_ad_slot as bid,
         |    exp_cvr,
         |    raw_cvr * 1.0 / 1000000 as raw_cvr,
         |    exp_ctr,
         |    media_appsid,
         |    cast(exp_cpm as double) / 1000000 as exp_cpm,
         |    hidden_tax,
         |    ocpc_step,
         |    deep_ocpc_step,
         |    ocpc_log,
         |    deep_ocpc_log,
         |    deep_cvr * 1.0 / 1000000 as exp_cvr2,
         |    deep_cpa,
         |    date,
         |    hour
         |FROM
         |    dl_cpc.ocpc_base_unionlog
         |WHERE
         |    `date` >= '$date1'
         |and isshow = 1
         |and conversion_goal > 0
         |and is_ocpc = 1
       """.stripMargin
    println(sqlRequest1)
    val clickData = spark.sql(sqlRequest1)

    // 关联浅层转化表
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  conversion_goal,
         |  conversion_from,
         |  1 as iscvr1
         |FROM
         |  dl_cpc.ocpc_cvr_log_hourly
         |WHERE
         |  date >= '$date1'
       """.stripMargin
    println(sqlRequest2)
    val cvData1 = spark.sql(sqlRequest2).distinct()

    // 关联深层转化表
    val sqlRequest3 =
      s"""
         |SELECT
         |  searchid,
         |  deep_conversion_goal,
         |  1 as iscvr2
         |FROM
         |  dl_cpc.ocpc_label_deep_cvr_hourly
         |WHERE
         |  date >= '$date1'
       """.stripMargin
    println(sqlRequest3)
    val cvData2 = spark.sql(sqlRequest3).distinct()


    // 数据关联
    val resultDF = clickData
      .join(cvData1, Seq("searchid", "conversion_goal", "conversion_from"), "left_outer")
      .join(cvData2, Seq("searchid", "deep_conversion_goal"), "left_outer")
      .na.fill(0, Seq("iscvr1", "iscvr2"))
      .withColumn("ocpc_log_dict", udfStringToMap()(col("ocpc_log")))
      .withColumn("deep_ocpc_log_dict", udfStringToMap()(col("deep_ocpc_log")))
      .selectExpr("searchid", "ideaid", "unitid", "userid", "adslot_type", "adslotid", "adclass", "conversion_goal", "conversion_from", "deep_conversion_goal", "cpa_check_priority", "is_deep_ocpc", "ocpc_expand", "isclick", "isshow", "price", "bid", "exp_cvr", "cast(raw_cvr as double) as raw_cvr", "exp_ctr", "media_appsid", "exp_cpm", "hidden_tax", "ocpc_step", "deep_ocpc_step", "ocpc_log", "deep_ocpc_log", "cast(exp_cvr2 as double) as exp_cvr2", "deep_cpa", "iscvr1", "iscvr2", "ocpc_log_dict", "deep_ocpc_log_dict", "date", "hour")

    resultDF

  }


}