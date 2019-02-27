package com.cpc.spark.OcpcProtoType.report

import com.cpc.spark.tools.OperateMySQL
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
    val version = args(2).toString
    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version")

    // 拉取点击、消费、转化等基础数据
    val baseData = getBaseData(date, hour, spark)

    // 分ideaid和conversion_goal统计数据
    val rawDataUnit = preprocessDataByUnit(baseData, date, hour, spark)
    val dataUnit = getDataByUnit(rawDataUnit, date, hour, spark)

    // 分conversion_goal统计数据
    val rawDataConversion = preprocessDataByConversion(dataUnit, date, hour, spark)
    val costDataConversion = preprocessCostByConversion(dataUnit, date, hour, spark)
    val dataConversion = getDataByConversion(rawDataConversion, costDataConversion, date, hour, spark)

    // 存储数据到hadoop
    saveDataToHDFS(dataUnit, dataConversion, "qtt_demo", date, hour, spark)

//    // 存储数据到mysql
//    saveDataToMysql(dataUnit, dataConversion, date, hour, spark)

  }

  def saveDataToMysql(dataUnit: DataFrame, dataConversion: DataFrame, date: String, hour: String, spark: SparkSession) = {
    val hourInt = hour.toInt
    // 详情表
    val dataUnitMysql = dataUnit
      .select("user_id", "idea_id", "conversion_goal", "step2_click_percent", "is_step2", "cpa_given", "cpa_real", "cpa_ratio", "is_cpa_ok", "impression", "click", "conversion", "ctr", "click_cvr", "show_cvr", "cost", "acp", "avg_k", "recent_k", "pre_cvr", "post_cvr", "q_factor", "acb", "auc")
      .na.fill(0, Seq("step2_click_percent", "is_step2", "cpa_given", "cpa_real", "cpa_ratio", "is_cpa_ok", "impression", "click", "conversion", "ctr", "click_cvr", "show_cvr", "cost", "acp", "avg_k", "recent_k", "pre_cvr", "post_cvr", "q_factor", "acb", "auc"))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hourInt))
    val reportTableIdea = "report2.report_ocpc_data_detail"
    val delSQLidea = s"delete from $reportTableIdea where `date` = '$date' and hour = $hourInt"

    OperateMySQL.update(delSQLidea) //先删除历史数据
    OperateMySQL.insert(dataUnitMysql, reportTableIdea) //插入数据

    // 汇总表
    val dataConversionMysql = dataConversion
      .select("conversion_goal", "total_adnum", "step2_adnum", "low_cpa_adnum", "high_cpa_adnum", "step2_cost", "step2_cpa_high_cost", "impression", "click", "conversion", "ctr", "click_cvr", "cost", "acp", "pre_cvr", "post_cvr", "q_factor", "acb", "auc")
      .na.fill(0, Seq("total_adnum", "step2_adnum", "low_cpa_adnum", "high_cpa_adnum", "step2_cost", "step2_cpa_high_cost", "impression", "click", "conversion", "ctr", "click_cvr", "cost", "acp", "pre_cvr", "post_cvr", "q_factor", "acb", "auc"))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hourInt))
    val reportTableConversion = "report2.report_ocpc_data_summary_v2"
    val delSQLconversion = s"delete from $reportTableConversion where `date` = '$date' and hour = $hourInt"

    OperateMySQL.update(delSQLconversion) //先删除历史数据
    OperateMySQL.insert(dataConversionMysql, reportTableConversion) //插入数据
  }

  def saveDataToHDFS(dataUnit: DataFrame, dataConversion: DataFrame, version: String, date: String, hour: String, spark: SparkSession) = {
    /*
    存储ideaid级别和conversion_goal级别的报表到hdfs
     */
    dataUnit
      .withColumn("version", lit(version))
      .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_detail_report_hourly_v3")
//      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_detail_report_hourly_v2")

    dataConversion
      .withColumn("version", lit(version))
      .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_summary_report_hourly_v3")
//      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_summary_report_hourly_v2")
  }

  def getDataByConversion(rawData: DataFrame, costData: DataFrame, date: String, hour: String, spark: SparkSession) = {
    /*
    1. 获取新增数据如auc
    2. 计算报表数据
    3. 数据关联并存储到结果表
     */

    // 获取新增数据如auc
    val aucData = spark
      .table("dl_cpc.ocpc_auc_report_summary_hourly")
      .where(s"`date`='$date' and `hour`='$hour' and version='qtt_demo'")
      .select("conversion_goal", "pre_cvr", "post_cvr", "q_factor", "acb", "auc")

    // 计算报表数据
    val hourInt = hour.toInt

    // 关联数据
    val resultDF = rawData
      .join(costData, Seq("conversion_goal"), "left_outer")
      .select("conversion_goal", "total_adnum", "step2_adnum", "low_cpa_adnum", "high_cpa_adnum", "step2_cost", "step2_cpa_high_cost", "impression", "click", "conversion", "ctr", "click_cvr", "cost", "acp")
      .join(aucData, Seq("conversion_goal"), "left_outer")
      .select("conversion_goal", "total_adnum", "step2_adnum", "low_cpa_adnum", "high_cpa_adnum", "step2_cost", "step2_cpa_high_cost", "impression", "click", "conversion", "ctr", "click_cvr", "cost", "acp", "pre_cvr", "post_cvr", "q_factor", "acb", "auc")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    resultDF
  }

  def preprocessCostByConversion(rawData: DataFrame, date: String, hour: String, spark: SparkSession) = {
    /*
    1. 从adv获取unitid与ideaid的对应list
    2. 给rawdata增加unitid维度
    3. 按照unitid维度统计cost
     */
    val data = spark
      .table("dl_cpc.ocpcv3_ctr_data_hourly")
      .where(s"`date`='$date' and `hour` <= '$hour' and media_appsid in ('80000001', '80000002')")
      .withColumn("idea_id", col("ideaid"))
      .withColumn("unit_id", col("unitid"))
      .select("idea_id", "unit_id")
      .distinct()

    val baseData = rawData
      .join(data, Seq("idea_id"), "inner")
      .select("user_id", "unit_id", "idea_id", "conversion_goal", "step2_click_percent", "is_step2", "cpa_given", "cpa_real", "cpa_ratio", "is_cpa_ok", "impression", "click", "conversion", "ctr", "click_cvr", "show_cvr", "cost", "acp", "avg_k", "recent_k", "pre_cvr", "post_cvr", "q_factor", "acb", "auc")
      .withColumn("click_cpa_given", col("cpa_given") * col("click"))

    baseData.write.mode("overwrite").saveAsTable("test.check_ocpc_report20190128")

    baseData.createOrReplaceTempView("base_data")

    // 计算step2的cost
    val sqlRequest1 =
      s"""
         |SELECT
         |  unit_id,
         |  conversion_goal,
         |  sum(click_cpa_given) / sum(click) as cpa_given,
         |  sum(cost) as cost,
         |  sum(conversion) as conversion
         |FROM
         |  base_data
         |WHERE
         |  is_step2=1
         |GROUP BY unit_id, conversion_goal
       """.stripMargin
    println(sqlRequest1)
    val unitidData = spark
      .sql(sqlRequest1)
      .withColumn("cost_given", col("cpa_given") * col("conversion") * 1.0 / 0.8)
      .withColumn("high_cpa_cost", col("cost") - col("cost_given"))
      .withColumn("high_cpa_cost", when(col("high_cpa_cost") <= 0, 0.0).otherwise(col("high_cpa_cost")))
    unitidData.createOrReplaceTempView("unitid_data")

    val sqlRequest2 =
      s"""
         |SELECT
         |  0 as conversion_goal,
         |  SUM(cost) as step2_cost,
         |  SUM(high_cpa_cost) as step2_cpa_high_cost
         |FROM
         |  unitid_data
       """.stripMargin
    println(sqlRequest2)
    val totalCost = spark.sql(sqlRequest2)

    val sqlRequest3 =
      s"""
         |SELECT
         |  conversion_goal,
         |  SUM(cost) as step2_cost,
         |  SUM(high_cpa_cost) as step2_cpa_high_cost
         |FROM
         |  unitid_data
         |GROUP BY conversion_goal
       """.stripMargin
    println(sqlRequest3)
    val splitCost = spark.sql(sqlRequest3)

    val resultDF = totalCost.union(splitCost).na.fill(0, Seq("step2_cost", "step2_cpa_high_cost"))

    resultDF
  }

  def preprocessDataByConversion(rawData: DataFrame, date: String, hour: String, spark: SparkSession) ={
    /*
    conversion_goal string  NULL
    total_adnum     bigint  NULL
    step2_adnum     bigint  NULL
    low_cpa_adnum   bigint  NULL
    high_cpa_adnum  bigint  NULL
    step2_cost      double  NULL
    step2_cpa_high_cost     double  NULL
    impression      bigint  NULL
    click   bigint  NULL
    conversion      bigint  NULL
    ctr     double  NULL
    click_cvr       double  NULL
    cost    double  NULL
    acp     double  NULL
    date    string  NULL
    hour    string  NULL
    "searchid", "ideaid", "userid", "isclick", "isshow", "price", "cpagiven", "bid", "kvalue", "conversion_goal", "ocpc_step", "iscvr1", "iscvr2", "iscvr3", "iscvr"
     */
    rawData.createOrReplaceTempView("raw_data")

    val sqlRequest0 =
      s"""
         |SELECT
         |  0 as conversion_goal,
         |  COUNT(1) as total_adnum,
         |  SUM(case when is_step2=1 then 1 else 0 end) as step2_adnum,
         |  SUM(case when is_cpa_ok=1 and is_step2=1 then 1 else 0 end) as low_cpa_adnum,
         |  SUM(case when is_cpa_ok=0 and is_step2=1 then 1 else 0 end) as high_cpa_adnum,
         |  SUM(impression) as impression,
         |  SUM(click) as click,
         |  SUM(conversion) as conversion,
         |  SUM(cost) as cost,
         |  SUM(cost) * 1.0 / SUM(click) as acp
         |FROM
         |  raw_data
       """.stripMargin
    println(sqlRequest0)
    val result0 = spark.sql(sqlRequest0)

    val sqlRequest1 =
      s"""
         |SELECT
         |  conversion_goal,
         |  COUNT(1) as total_adnum,
         |  SUM(case when is_step2=1 then 1 else 0 end) as step2_adnum,
         |  SUM(case when is_cpa_ok=1 and is_step2=1 then 1 else 0 end) as low_cpa_adnum,
         |  SUM(case when is_cpa_ok=0 and is_step2=1 then 1 else 0 end) as high_cpa_adnum,
         |  SUM(impression) as impression,
         |  SUM(click) as click,
         |  SUM(conversion) as conversion,
         |  SUM(cost) as cost,
         |  SUM(cost) * 1.0 / SUM(click) as acp
         |FROM
         |  raw_data
         |GROUP BY conversion_goal
       """.stripMargin
    println(sqlRequest1)
    val result1 = spark.sql(sqlRequest1)

    val resultDF = result0
      .union(result1)
      .withColumn("ctr", col("click") * 1.0 / col("impression"))
      .withColumn("click_cvr", col("conversion") * 1.0 / col("click"))
      .withColumn("click_cvr", when(col("click")===0, 1).otherwise(col("click_cvr")))
      .withColumn("acp", col("cost") * 1.0 / col("click"))
      .withColumn("acp", when(col("click")===0, 0).otherwise(col("acp")))

    resultDF
  }

  def getDataByUnit(rawData: DataFrame, date: String, hour: String, spark: SparkSession) = {
    /*
    1. 获取新增数据如auc
    2. 计算报表数据
    3. 数据关联并存储到结果表
     */

    // 获取新增数据如auc
    val aucData = spark
      .table("dl_cpc.ocpc_auc_report_detail_hourly")
      .where(s"`date`='$date' and `hour`='$hour' and version = 'qtt_demo'")
      .withColumn("unitid", col("identifier"))
      .selectExpr("cast(unitid as int) unitid", "userid", "conversion_goal", "pre_cvr", "post_cvr", "q_factor", "acb", "auc")

    // 计算报表数据
    val resultDF = rawData
      .withColumn("unit_id", col("unitid"))
      .withColumn("user_id", col("userid"))
      .withColumn("step2_click_percent", col("step2_percent"))
      .withColumn("is_step2", when(col("step2_percent")===1, 1).otherwise(0))
      .withColumn("cpa_ratio", when(col("cvr_cnt").isNull || col("cvr_cnt") === 0, 0.0).otherwise(col("cpa_given") * 1.0 / col("cpa_real")))
      .withColumn("is_cpa_ok", when(col("cpa_ratio")>=0.8, 1).otherwise(0))
      .withColumn("impression", col("show_cnt"))
      .withColumn("click", col("ctr_cnt"))
      .withColumn("conversion", col("cvr_cnt"))
      .withColumn("ctr", col("click") * 1.0 / col("impression"))
      .withColumn("click_cvr", col("conversion") * 1.0 / col("click"))
      .withColumn("show_cvr", col("conversion") * 1.0 / col("impression"))
      .withColumn("cost", col("price") * col("click"))
      .withColumn("acp", col("price"))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("recent_k", when(col("recent_k").isNull, 0.0).otherwise(col("recent_k")))
      .withColumn("cpa_real", when(col("cpa_real").isNull, 9999999.0).otherwise(col("cpa_real")))
//      .select("user_id", "idea_id", "conversion_goal", "step2_click_percent", "is_step2", "cpa_given", "cpa_real", "cpa_ratio", "is_cpa_ok", "impression", "click", "conversion", "ctr", "click_cvr", "show_cvr", "cost", "acp", "avg_k", "recent_k", "date", "hour")
      .join(aucData, Seq("unitid", "userid", "conversion_goal"), "left_outer")
      .select("user_id", "unit_id", "conversion_goal", "step2_click_percent", "is_step2", "cpa_given", "cpa_real", "cpa_ratio", "is_cpa_ok", "impression", "click", "conversion", "ctr", "click_cvr", "show_cvr", "cost", "acp", "avg_k", "recent_k", "pre_cvr", "post_cvr", "q_factor", "acb", "auc", "date", "hour")

    resultDF.show(10)

    resultDF

  }

  def preprocessDataByUnit(rawData: DataFrame, date: String, hour: String, spark: SparkSession) = {
    /*
    //    ideaid  int     NULL
    //    userid  int     NULL
    //    conversion_goal string  NULL
    //    step2_percent   double  NULL
    //    cpa_given       double  NULL
    //    cpa_real        double  NULL
    //    pcvr    double  NULL
    //    ctr     double  NULL
    //    click_cvr       double  NULL
    //    show_cvr        double  NULL
    //    price   double  NULL
    //    show_cnt        bigint  NULL
    //    ctr_cnt bigint  NULL
    //    cvr_cnt bigint  NULL
    //    avg_k   double  NULL
    //    recent_k        double  NULL
    "searchid", "ideaid", "userid", "isclick", "isshow", "price", "cpagiven", "bid", "kvalue", "conversion_goal", "ocpc_step", "iscvr1", "iscvr2", "iscvr3", "iscvr"
     */
    rawData.createOrReplaceTempView("raw_data")


    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  userid,
         |  conversion_goal,
         |  sum(case when ocpc_step=2 then 1 else 0 end) * 1.0 / count(1) as step2_percent,
         |  sum(case when isclick=1 then cpagiven else 0 end) * 1.0 / sum(isclick) as cpa_given,
         |  sum(case when isclick=1 then price else 0 end) * 1.0 / sum(iscvr) as cpa_real,
         |  sum(case when isclick=1 then exp_cvr else 0 end) * 1.0 / sum(isclick) as pcvr,
         |  sum(isclick) * 1.0 / sum(isshow) as ctr,
         |  sum(iscvr) * 1.0 / sum(isclick) as click_cvr,
         |  sum(iscvr) * 1.0 / sum(isshow) as show_cvr,
         |  sum(case when isclick=1 then price else 0 end) * 1.0 / sum(isclick) as price,
         |  sum(isshow) as show_cnt,
         |  sum(isclick) as ctr_cnt,
         |  sum(iscvr) as cvr_cnt,
         |  sum(case when isclick=1 then kvalue else 0 end) * 1.0 / sum(isclick) as avg_k,
         |  sum(case when isclick=1 and hr='$hour' then kvalue else 0 end) * 1.0 / sum(case when hr='$hour' then isclick else 0 end) as recent_k
         |FROM
         |  raw_data
         |GROUP BY unitid, userid, conversion_goal
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)

    resultDF
  }

  def getBaseData(date: String, hour: String, spark: SparkSession) = {
    /**
      * 重新计算抽取全天截止当前时间的数据日志
      */

    // 抽取基础数据：所有跑ocpc的广告主
    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  userid,
         |  isclick,
         |  isshow,
         |  price,
         |  exp_cvr,
         |  exp_ctr,
         |  cast(ocpc_log_dict['cpagiven'] as double) as cpagiven,
         |  cast(ocpc_log_dict['dynamicbid'] as double) as bid,
         |  cast(ocpc_log_dict['kvalue'] as double) as kvalue,
         |  cast(ocpc_log_dict['conversiongoal'] as int) as conversion_goal,
         |  cast(ocpc_log_dict['ocpcstep'] as int) as ocpc_step,
         |  hour as hr
         |FROM
         |  dl_cpc.ocpc_filter_unionlog
         |WHERE
         |  `dt`='$date' and `hour` <= '$hour'
         |AND
         |    is_ocpc=1
         |AND
         |    media_appsid  in ("80000001", "80000002")
         |AND
         |    isshow=1
       """.stripMargin
    println(sqlRequest)
    val rawData = spark.sql(sqlRequest)


    // 关联转化表
    val selectCondition = s"`date`='$date'"
    // cvr1
    val cvr1Data = spark
      .table("dl_cpc.ocpc_label_cvr_hourly")
      .where(selectCondition)
      .filter(s"cvr_goal = 'cvr1'")
      .select("searchid")
      .withColumn("iscvr1", lit(1))
      .distinct()

    // cvr2
    val cvr2Data = spark
      .table("dl_cpc.ocpc_label_cvr_hourly")
      .where(selectCondition)
      .filter(s"cvr_goal = 'cvr2'")
      .select("searchid")
      .withColumn("iscvr2", lit(1))
      .distinct()

    // cvr3
    val cvr3Data = spark
      .table("dl_cpc.ocpc_label_cvr_hourly")
      .where(selectCondition)
      .filter(s"cvr_goal = 'cvr3'")
      .select("searchid")
      .withColumn("iscvr3", lit(1))
      .distinct()

    // 数据关联
    val resultDF = rawData
      .join(cvr1Data, Seq("searchid"), "left_outer")
      .join(cvr2Data, Seq("searchid"), "left_outer")
      .join(cvr3Data, Seq("searchid"), "left_outer")
      .withColumn("iscvr", when(col("conversion_goal") === 1, col("iscvr1")).otherwise(when(col("conversion_goal") === 2, col("iscvr2")).otherwise(col("iscvr3"))))
      .select("searchid", "unitid", "userid", "isclick", "isshow", "price", "exp_ctr", "exp_cvr", "cpagiven", "bid", "kvalue", "conversion_goal", "ocpc_step", "hr", "iscvr1", "iscvr2", "iscvr3", "iscvr")

    resultDF.show(10)

    resultDF

  }


}