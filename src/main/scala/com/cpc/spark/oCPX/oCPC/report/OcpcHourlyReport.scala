package com.cpc.spark.oCPX.oCPC.report

import com.cpc.spark.oCPX.OcpcTools._
import com.cpc.spark.tools.testOperateMySQL
import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcHourlyReport {
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
    val hour = args(1).toString
    println("parameters:")
    println(s"date=$date, hour=$hour")

    // 拉取点击、消费、转化等基础数据
    val rawData = getBaseData(date, hour, spark)

    // 分ideaid和conversion_goal统计数据
    val baseData = calculateBaseData(rawData, spark)

    // 为邮件准备临时表
    if (hour == "23") {
      val ideaData = calculateIdea(baseData, spark)
      ideaData
        .withColumn("date", lit(date))
        .withColumn("hour", lit(hour))
        .repartition(5)
        .write.mode("overwrite").saveAsTable("test.ocpc_hourly_idea_report_email")

      val unitData = calculateUnit(baseData, spark)
      unitData
        .withColumn("date", lit(date))
        .withColumn("hour", lit(hour))
        .repartition(5)
        .write.mode("overwrite").saveAsTable("test.ocpc_hourly_unit_report_email")

      val userData = calcualteUser(unitData, spark)
      userData
        .withColumn("date", lit(date))
        .withColumn("hour", lit(hour))
        .repartition(5)
        .write.mode("overwrite").saveAsTable("test.ocpc_hourly_user_report_email")

      val industry = calculateIndustry(unitData, spark)
      industry
        .withColumn("date", lit(date))
        .withColumn("hour", lit(hour))
        .repartition(5)
        .write.mode("overwrite").saveAsTable("test.ocpc_hourly_industry_report_email")
    }

    // 存储数据到hadoop
    saveDataToHDFS(baseData, date, hour, spark)

    // 存储数据到mysql
    saveDataToMysql(baseData, date, hour, spark)

  }

  def calculateIndustry(baseData: DataFrame, spark: SparkSession) = {
    baseData.createOrReplaceTempView("base_data")
    val sqlRequest =
      s"""
         |SELECT
         |    media,
         |    industry,
         |    conversion_goal,
         |    is_hidden,
         |    sum(show) as show,
         |    sum(click) as click,
         |    sum(cv) as cv,
         |    sum(pre_ctr * show) * 1.0 / sum(show) as pre_ctr,
         |    sum(click) * 1.0 / sum(show) as post_ctr,
         |    sum(pre_cvr * click) * 1.0 / sum(click) as pre_cvr,
         |    sum(cali_precvr * click) * 1.0 / sum(click) as cali_precvr,
         |    sum(cv) * 1.0 / sum(click) as post_cvr,
         |    sum(cost) as cost,
         |    sum(pay) as pay,
         |    sum(acp * click) * 1.0 / sum(click) as acp,
         |    sum(acb * click) * 1.0 / sum(click) as acb,
         |    sum(cpagiven * click) * 1.0 / sum(click) as cpagiven,
         |    sum(acp * click) * 1.0 / sum(cv) as cpareal,
         |    sum(cpasuggest * click) * 1.0 / sum(click) as cpasuggest,
         |    sum(jfb_factor * click) * 1.0 / sum(click) as jfb_factor,
         |    sum(cvr_factor * click) * 1.0 / sum(click) as cvr_factor,
         |    sum(cali_postcvr * click) * 1.0 / sum(click) as cali_postcvr,
         |    sum(smooth_factor * click) * 1.0 / sum(click) as smooth_factor
         |FROM
         |    base_data
         |GROUP BY media, industry, conversion_goal, is_hidden
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest).cache()
    println("industry data:")
    data.show(10)
    data
  }

  def calcualteUser(baseData: DataFrame, spark: SparkSession) = {
    baseData.createOrReplaceTempView("base_data")
    val sqlRequest =
      s"""
         |SELECT
         |    userid,
         |    adclass,
         |    media,
         |    industry,
         |    conversion_goal,
         |    is_hidden,
         |    sum(show) as show,
         |    sum(click) as click,
         |    sum(cv) as cv,
         |    sum(pre_ctr * show) * 1.0 / sum(show) as pre_ctr,
         |    sum(click) * 1.0 / sum(show) as post_ctr,
         |    sum(pre_cvr * click) * 1.0 / sum(click) as pre_cvr,
         |    sum(cali_precvr * click) * 1.0 / sum(click) as cali_precvr,
         |    sum(cv) * 1.0 / sum(click) as post_cvr,
         |    sum(cost) as cost,
         |    sum(pay) as pay,
         |    sum(acp * click) * 1.0 / sum(click) as acp,
         |    sum(acb * click) * 1.0 / sum(click) as acb,
         |    sum(cpagiven * click) * 1.0 / sum(click) as cpagiven,
         |    sum(acp * click) * 1.0 / sum(cv) as cpareal,
         |    sum(cpasuggest * click) * 1.0 / sum(click) as cpasuggest,
         |    sum(jfb_factor * click) * 1.0 / sum(click) as jfb_factor,
         |    sum(cvr_factor * click) * 1.0 / sum(click) as cvr_factor,
         |    sum(cali_postcvr * click) * 1.0 / sum(click) as cali_postcvr,
         |    sum(smooth_factor * click) * 1.0 / sum(click) as smooth_factor
         |FROM
         |    base_data
         |GROUP BY userid, adclass, media, industry, conversion_goal, is_hidden
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest).cache()
    println("user data:")
    data.show(10)
    data
  }

  def calculateUnit(baseData: DataFrame, spark: SparkSession) = {
    baseData.createOrReplaceTempView("base_data")
    val sqlRequest =
      s"""
         |SELECT
         |    unitid,
         |    userid,
         |    adclass,
         |    media,
         |    industry,
         |    conversion_goal,
         |    is_hidden,
         |    sum(show) as show,
         |    sum(click) as click,
         |    sum(cv) as cv,
         |    sum(total_prectr) * 1.0 / sum(show) as pre_ctr,
         |    sum(click) * 1.0 / sum(show) as post_ctr,
         |    sum(total_precvr) * 1.0 / sum(click) as pre_cvr,
         |    sum(total_calipcvr) * 1.0 / sum(click) as cali_precvr,
         |    sum(cv) * 1.0 / sum(click) as post_cvr,
         |    sum(total_price) * 0.01 as cost,
         |    (case
         |      when sum(total_price) <= 1.2 * sum(cv) * sum(total_cpagiven) / sum(click) then 0
         |      else sum(total_price) - 1.2 * sum(cv) * sum(total_cpagiven) / sum(click)
         |    end) * 0.01 as pay,
         |    sum(total_price) * 1.0 / sum(click) as acp,
         |    sum(total_bid) * 1.0 / sum(click) as acb,
         |    sum(total_cpagiven) * 1.0 / sum(click) as cpagiven,
         |    sum(total_price) * 1.0 / sum(cv) as cpareal,
         |    sum(total_cpasuggest) * 1.0 / sum(click) as cpasuggest,
         |    sum(total_jfbfactor) * 1.0 / sum(click) as jfb_factor,
         |    sum(total_cvrfactor) * 1.0 / sum(click) as cvr_factor,
         |    sum(total_calipostcvr) * 1.0 / sum(click) as cali_postcvr,
         |    sum(total_smooth_factor) * 1.0 / sum(click) as smooth_factor
         |FROM
         |    base_data
         |GROUP BY unitid, userid, adclass, media, industry, conversion_goal, is_hidden
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    val result = data
      .na.fill(0, Seq("cv"))
//      .withColumn("pay", udfCalculatePay()(col("cost"), col("cv"), col("cpagiven")))
    println("unit data:")
    result.show(10)
    result
  }


  def calculateIdea(baseData: DataFrame, spark: SparkSession) = {
    baseData.createOrReplaceTempView("base_data")
    val sqlRequest =
      s"""
         |SELECT
         |    ideaid,
         |    unitid,
         |    userid,
         |    adclass,
         |    media,
         |    industry,
         |    conversion_goal,
         |    is_hidden,
         |    sum(show) as show,
         |    sum(click) as click,
         |    sum(cv) as cv,
         |    sum(total_prectr) * 1.0 / sum(show) as pre_ctr,
         |    sum(click) * 1.0 / sum(show) as post_ctr,
         |    sum(total_precvr) * 1.0 / sum(click) as pre_cvr,
         |    sum(total_calipcvr) * 1.0 / sum(click) as cali_precvr,
         |    sum(cv) * 1.0 / sum(click) as post_cvr,
         |    sum(total_price) * 0.01 as cost,
         |    sum(total_price) * 1.0 / sum(click) as acp,
         |    sum(total_bid) * 1.0 / sum(click) as acb,
         |    sum(total_cpagiven) * 1.0 / sum(click) as cpagiven,
         |    sum(total_price) * 1.0 / sum(cv) as cpareal,
         |    sum(total_cpasuggest) * 1.0 / sum(click) as cpasuggest,
         |    sum(total_jfbfactor) * 1.0 / sum(click) as jfb_factor,
         |    sum(total_cvrfactor) * 1.0 / sum(click) as cvr_factor,
         |    sum(total_calipostcvr) * 1.0 / sum(click) as cali_postcvr,
         |    sum(total_smooth_factor) * 1.0 / sum(click) as smooth_factor
         |FROM
         |    base_data
         |GROUP BY ideaid, unitid, userid, adclass, media, industry, conversion_goal, is_hidden
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest).cache()
    println("idea_data:")
    data.show(10)
    data
  }


  def saveDataToHDFS(data: DataFrame, date: String, hour: String, spark: SparkSession) = {
    val resultDF = data
      .withColumn("date", lit(date))
      .withColumn("hour", col("hr"))
      .select("ideaid", "unitid", "userid", "adclass", "conversion_goal", "industry", "media", "show", "click", "cv", "total_price", "total_bid", "total_precvr", "total_prectr", "total_cpagiven", "total_jfbfactor", "total_cvrfactor", "total_calipcvr", "total_calipostcvr", "total_cpasuggest", "total_smooth_factor", "is_hidden", "date", "hour")


    resultDF
      .repartition(5)
      //      .write.mode("overwrite").insertInto("test.ocpc_report_base_hourly")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_report_base_hourly")
  }

  def calculateBaseData(rawData: DataFrame, spark: SparkSession) = {
    rawData.createOrReplaceTempView("raw_data")
    val sqlRequest =
      s"""
         |SELECT
         |  ideaid,
         |  unitid,
         |  userid,
         |  adclass,
         |  conversion_goal,
         |  industry,
         |  media,
         |  hr,
         |  cast(ocpc_log_dict['IsHiddenOcpc'] as int) as is_hidden,
         |  sum(isshow) as show,
         |  sum(isclick) as click,
         |  sum(case when isclick=1 then iscvr else 0) as cv,
         |  sum(case when isclick=1 then price else 0 end) as total_price,
         |  sum(case when isclick=1 then bid else 0 end) as total_bid,
         |  sum(case when isclick=1 then exp_cvr else 0 end) * 1.0 as total_precvr,
         |  sum(case when isshow=1 then exp_ctr else 0 end) * 1.0 as total_prectr,
         |  sum(case when isclick=1 then cast(ocpc_log_dict['cpagiven'] as double) else 0 end) as total_cpagiven,
         |  sum(case when isclick=1 then cast(ocpc_log_dict['kvalue'] as double) else 0 end) * 1.0 as total_jfbfactor,
         |  sum(case when isclick=1 then cast(ocpc_log_dict['cvrCalFactor'] as double) else 0 end) * 1.0 as total_cvrfactor,
         |  sum(case when isclick=1 then cast(ocpc_log_dict['pcvr'] as double) else 0 end) * 1.0 as total_calipcvr,
         |  sum(case when isclick=1 then cast(ocpc_log_dict['postCvr'] as double) else 0 end) * 1.0 as total_calipostcvr,
         |  sum(case when isclick=1 then cast(ocpc_log_dict['CpaSuggest'] as double) else 0 end) as total_cpasuggest,
         |  sum(case when isclick=1 then cast(ocpc_log_dict['smoothFactor'] as double) else 0 end) * 1.0 as total_smooth_factor
         |FROM
         |  raw_data
         |GROUP BY ideaid, unitid, userid, adclass, conversion_goal, industry, media, hr, cast(ocpc_log_dict['IsHiddenOcpc'] as int)
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest).cache()
    println("base_data:")
    data.show(10)
    data
  }

  def getBaseData(date: String, hour: String, spark: SparkSession) = {
    /**
      * 重新计算抽取全天截止当前时间的数据日志
      */
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias.total.media_selection"
    val mediaSelection = conf.getString(conf_key)

    // 抽取基础数据：所有跑ocpc的广告主
    val sqlRequest1 =
      s"""
         |SELECT
         |    searchid,
         |    ideaid,
         |    unitid,
         |    userid,
         |    adslot_type,
         |    adclass,
         |    conversion_goal,
         |    isclick,
         |    isshow,
         |    price,
         |    bid_discounted_by_ad_slot as bid,
         |    exp_cvr,
         |    exp_ctr,
         |    media_appsid,
         |    ocpc_log,
         |    hour as hr
         |FROM
         |    dl_cpc.ocpc_base_unionlog
         |WHERE
         |    `date` = '$date'
         |and `hour` <= '$hour'
         |and ocpc_step = 2
         |and length(ocpc_log) > 0
         |and $mediaSelection
         |and round(adclass/1000) != 132101  --去掉互动导流
         |and isshow = 1
         |and ideaid > 0
         |and adsrc = 1
         |and adslot_type in (1,2,3)
         |and searchid is not null
         |and conversion_goal > 0
       """.stripMargin
    println(sqlRequest1)
    val clickData = spark
      .sql(sqlRequest1)
      .withColumn("cvr_goal", udfConcatStringInt("cvr")(col("conversion_goal")))
      .withColumn("media", udfDetermineMedia()(col("media_appsid")))
      .withColumn("industry", udfDetermineIndustry()(col("adslot_type"), col("adclass")))
      .withColumn("ocpc_log_dict", udfStringToMap()(col("ocpc_log")))


    // 关联转化表
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  label as iscvr,
         |  cvr_goal
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  date = '$date'
       """.stripMargin
    println(sqlRequest2)
    val cvData = spark.sql(sqlRequest2)

    // 数据关联
    val resultDF = clickData
      .join(cvData, Seq("searchid", "cvr_goal"), "left_outer")
      .na.fill(0, Seq("iscvr"))

    resultDF

  }

  def saveDataToMysql(baseData: DataFrame, date: String, hour: String, spark: SparkSession) = {
    /*
    ideaid                  int,
    unitid                  int,
    userid                  int,
    adclass                 int,
    conversion_goal         int,
    industry                string,
    media                   string,
    show                    bigint,
    click                   bigint,
    cv                      bigint,
    total_price             bigint,
    total_bid               bigint,
    total_precvr            double,
    total_prectr            double,
    total_cpagiven          bigint,
    total_jfbfactor         double,
    total_cvrfactor         double,
    total_calipcvr          double,
    total_calipostcvr       double,
    total_cpasuggest        double,
    total_smooth_factor     double,
    is_hidden               int
     */
    // unitid详情表
    val dataUnitMysql = baseData
      .withColumn("date", lit(date))
      .withColumn("hour", col("hr"))
      .withColumn("impression", col("show"))
      .select("ideaid", "unitid", "userid", "adclass", "conversion_goal", "industry", "media", "impression", "click", "cv", "total_price", "total_bid", "total_precvr", "total_prectr", "total_cpagiven", "total_jfbfactor", "total_cvrfactor", "total_calipcvr", "total_calipostcvr", "total_cpasuggest", "total_smooth_factor", "is_hidden", "date", "hour")

    val reportTableUnit = "report2.ocpc_report_base_hourly"
    val delSQLunit = s"delete from $reportTableUnit where `date` = '$date'"

    testOperateMySQL.update(delSQLunit) //先删除历史数据
    testOperateMySQL.insert(dataUnitMysql, reportTableUnit) //插入数据


  }

}