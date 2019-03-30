package com.cpc.spark.OcpcProtoType.report_qtt

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object UnitAaReport {
  def main(args: Array[String]): Unit = {
    val date = args(0).toString
    val hour = args(1).toString
    val spark = SparkSession.builder().appName("UnitAaReport").enableHiveSupport().getOrCreate()
    val baseDataDF = getBaseData(date, hour, spark)
    println("-----has got base data from base unionlog-----")
    val suggestCpaDF = getSuggestCpa(date, hour, spark)
    println("-----has got suggest cpa-----")
    val aucDF = getAuc(date, hour, spark)
    println("-----has got auc-----")
    calculateIndexValue(date, hour, baseDataDF, suggestCpaDF, aucDF, spark)
    println("-----successful------")
  }

  // 首先从base表里获取基础字段
  def getBaseData(date: String, hour: String, spark: SparkSession): DataFrame ={
    var sql1 =
      s"""
        |select
        |    searchid,
        |    unitid,
        |    userid,
        |    adslot_type,
        |    (case when (cast(adclass as string) like "134%" or cast(adclass as string) like "107%") then 'elds'
        |      when cast(adclass as string) like "100%" then 'app'
        |      when adclass in (110110100, 125100100) then 'wzcp'
        |      else 'others' end) as industry,
        |    is_ocpc,
        |    isclick,
        |    bid,
        |    isshow,
        |    price,
        |    exp_cvr,
        |    ocpc_log,
        |    `date`,
        |    hour
        |from
        |    dl_cpc.ocpc_base_unionlog
        |where
        |    media_appsid  in ("80000001", "80000002")
        |and
        |    isshow = 1
        |and
        |    antispam = 0
        |and
        |    adsrc = 1
        |and
        |    `date` = '$date'
      """.stripMargin
    // 当hour等于-1时，表示取全天的数据
    if("all".equals(hour) == false) sql1 +=  s" and hour = '$hour'"
    println(sql1)
    spark.sql(sql1)
      .withColumn("ocpc_log_dict", udfStringToMap()(col("ocpc_log")))
      .createOrReplaceTempView("temp_table")

    val sql2 =
      s"""
        |select
        |    a.*,
        |    (case when cast(a.ocpc_log_dict['conversiongoal'] as int) = 1 then b.iscvr1
        |          when cast(a.ocpc_log_dict['conversiongoal'] as int) = 2 then c.iscvr2
        |          else d.iscvr3 end) as iscvr
        |from
        |    temp_table a
        |left join
        |    (select
        |        searchid,
        |        label2 as iscvr1
        |    from
        |        dl_cpc.ml_cvr_feature_v1
        |    where
        |        `date` = '$date'
        |    and
        |        label2 = 1
        |    and
        |        label_type in (1, 2, 3, 4, 5)
        |    group by
        |        searchid,
        |        label2) as b
        |on
        |    a.searchid = b.searchid
        |left join
        |    (select
        |        searchid,
        |        label as iscvr2
        |    from
        |        dl_cpc.ml_cvr_feature_v2
        |    where
        |        `date` = '$date'
        |    and
        |        label = 1
        |    group by
        |        searchid,
        |        label) as c
        |on
        |    a.searchid = c.searchid
        |left join
        |    (select
        |        searchid,
        |        1 as iscvr3
        |    from
        |        dl_cpc.site_form_unionlog
        |    WHERE
        |        `date` = '$date'
        |    and
        |        ideaid > 0
        |    and
        |        searchid is not null
        |    group by
        |        searchid) as d
        |on
        |    a.searchid = d.searchid
      """.stripMargin
    println("--------------")
    println(sql2)
    val baseDataDF = spark.sql(sql2)
    baseDataDF
  }

  // 获取suggest_cpa
  def getSuggestCpa(date: String, spark: SparkSession): DataFrame ={
     var subQuery =
       s"""
         |select
         |    unitid,
         |    userid,
         |    cpa,
         |    row_number() over(partition by unitid, userid order by cost desc) as row_num
         |from
         |    dl_cpc.ocpc_suggest_cpa_recommend_hourly
         |where
         |   version = 'qtt_demo'
         |and
         |   `date` = '$date'
         |and
         |    hour = '06'
       """.stripMargin

    val sql =
      s"""
        |select
        |    a.unitid,
        |    a.userid,
        |    round(a.cpa * 0.01, 4) as suggest_cpa
        |from
        |    ($subQuery) a
        |where
        |    a.row_num = 1
      """.stripMargin
    println(sql)

    val suggestCpaDF = spark.sql(sql)
    suggestCpaDF
   }

  // 获取auc
  def getAuc(date: String, hour: String, spark: SparkSession): DataFrame ={
    var sql1 =
      s"""
        |select
        |    a.unitid,
        |    a.userid,
        |    1 as conversion_goal,
        |    a.exp_cvr,
        |    b.iscvr1 as iscvr
        |from
        |    dl_cpc.ocpc_base_unionlog a
        |left join
        |    (select
        |        searchid,
        |        label2 as iscvr1
        |    from
        |        dl_cpc.ml_cvr_feature_v1
        |    where
        |        `date` = '$date'
        |    and
        |        label2 = 1
        |    and
        |        label_type in (1, 2, 3, 4, 5)
        |    ) as b
        |on
        |    a.searchid = b.searchid
        |where
        |    a.`date` = '$date'
        |and
        |    a.media_appsid  in ("80000001", "80000002")
        |and
        |    a.isshow = 1
        |and
        |    a.antispam = 0
        |and
        |    a.adslot_type in (1, 2, 3)
        |and
        |    a.adsrc = 1
        |and
        |    (a.charge_type is null or a.charge_type = 1)
      """.stripMargin
    if("all".equals(hour) == false) sql1 += s" and a.hour = '$hour'"
    println("==sql1===")
    println(sql1)
    val dataFrame1 = spark.sql(sql1)

    var sql2 =
      s"""
        |select
        |    a.unitid,
        |    a.userid,
        |    2 as conversion_goal,
        |    a.exp_cvr,
        |    b.iscvr2 as iscvr
        |from
        |    dl_cpc.ocpc_base_unionlog a
        |left join
        |    (select
        |        searchid,
        |        label as iscvr2
        |    from
        |        dl_cpc.ml_cvr_feature_v2
        |    where
        |        `date` = '$date'
        |    and
        |        label = 1
        |    ) as b
        |on
        |    a.searchid = b.searchid
        |where
        |    a.`date` = '$date'
        |and
        |    a.media_appsid  in ("80000001", "80000002")
        |and
        |    a.isshow = 1
        |and
        |    a.antispam = 0
        |and
        |    a.adslot_type in (1, 2, 3)
        |and
        |    a.adsrc = 1
        |and
        |    (a.charge_type is null or a.charge_type = 1)
      """.stripMargin
    if("all".equals(hour) == false) sql2 += s" and a.hour = '$hour'"
    println("==sql2===")
    println(sql2)
    val dataFrame2 = spark.sql(sql2)

    var sql3 =
      s"""
        |select
        |    a.unitid,
        |    a.userid,
        |    3 as conversion_goal,
        |    a.exp_cvr,
        |    b.iscvr3 as iscvr
        |from
        |    dl_cpc.ocpc_base_unionlog a
        |left join
        |    (select
        |        searchid,
        |        1 as iscvr3
        |    from
        |        dl_cpc.site_form_unionlog
        |    where
        |        `date` = '$date'
        |    and
        |        ideaid > 0
        |    and
        |        searchid is not null
        |    ) as b
        |on
        |    a.searchid = b.searchid
        |where
        |    `date` = '$date'
        |and
        |    a.media_appsid  in ("80000001", "80000002")
        |and
        |    a.isshow = 1
        |and
        |    a.antispam = 0
        |and
        |    a.adslot_type in (1, 2, 3)
        |and
        |    a.adsrc = 1
      """.stripMargin
    if("all".equals(hour) == false) sql3 += s" and a.hour = '$hour'"
    println("==sql3===")
    println(sql3)
    val dataFrame3 = spark.sql(sql3)

    val dataFrame = dataFrame1.union(dataFrame2).union(dataFrame3)
    val aucDF = OcpcHourlyAucReport.calculateAUCbyUnitid(dataFrame, date, hour, spark)
    aucDF
  }

  // 计算基本统计值
  def calculateIndexValue(date: String, hour: String, baseDataDF: DataFrame, suggestCpaDF: DataFrame,
                          aucDF: DataFrame, spark: SparkSession): Unit ={
    baseDataDF.createOrReplaceTempView("base_data_table")
    suggestCpaDF.createOrReplaceTempView("suggest_cpa_table")
    aucDF.createOrReplaceTempView("auc_table")

    val sql =
      s"""
        |select
        |    unitid,
        |    userid,
        |    industry,
        |    round(sum(case when isclick = 1 then is_ocpc else 0 end) / sum(isclick), 4) as put_type,
        |    adslot_type,
        |    cast(ocpc_log_dict['conversiongoal'] as int) as conversion_goal,
        |    sum(iscvr) as cv,
        |    sum(isclick) as click,
        |    sum(isshow) as show,
        |    sum(case when isclick = 1 then price else 0 end) * 0.01 as cost,
        |    round(sum(case when isclick = 1 then price else 0 end) * 10.0 / sum(isshow), 4) as cpm,
        |    sum(case when isclick = 1 then exp_cvr else 0 end) * 1.0 / sum(isclick) as exp_cvr,
        |    round(sum(case when isclick = 1 then cast(ocpc_log_dict['pcvr'] as double) else 0 end) * 1.0 / sum(isclick), 4) as pre_cvr,
        |    round(sum(iscvr) * 1.0 / sum(isclick), 4) as post_cvr,
        |    round(sum(case when isclick = 1 then price else 0 end) / sum(isclick), 4) as cost_of_every_click,
        |    round(sum(case when isclick = 1 and length(ocpc_log) = 0 then bid
        |                   when isclick = 1 and length(ocpc_log) > 0 then cast(ocpc_log_dict['bid'] as double)
        |                   else 0 end) / sum(isclick), 4) as bid_of_every_click,
        |    round(sum(case when isclick = 1 then price else 0 end) / sum(iscvr), 4) as cpa_real,
        |    round(sum(case when isclick = 1 and length(ocpc_log) > 0 then cast(ocpc_log_dict['capgiven'] as double)
        |                   else 0 end) / sum(isclick), 4) as cpa_given,
        |    round(sum(case when isclick = 1 and length(ocpc_log) > 0 and ocpc_log_dict['IsHiddenOcpc'] = '1' then price
        |                   else 0 end) / sum(case when isclick = 1 then price else 0 end), 4) as hidden_cost_ratio,
        |    round(sum(case when isclick = 1 and length(ocpc_log) > 0 then cast(ocpc_log_dict['kvalue'] as double)
        |                   else 0 end) / sum(isclick), 4) as kvalue,
        |    max(case when isclick = 1 and length(ocpc_log) > 0 then cast(ocpc_log_dict['budget'] as double) else 0 end) as budget,
        |    round(case when (max(case when isclick = 1 and length(ocpc_log) > 0 then cast(ocpc_log_dict['budget'] as double) else 0 end)) = 0 then 0
        |               else (sum(case when isclick = 1 then price else 0 end)
        |                     / max(case when isclick = 1 and length(ocpc_log) > 0 then cast(ocpc_log_dict['budget'] as double) else 0 end)) end, 4)
        |                    as cost_budget_ratio
        |from
        |    base_data_table
        |group by
        |    unitid,
        |    userid,
        |    industry,
        |    adslot_type,
        |    cast(ocpc_log_dict['conversiongoal'] as int)
      """.stripMargin
    val dataFrame = spark.sql(sql)
    println(sql)
    val indexValueDF = dataFrame
      .join(suggestCpaDF, Seq("unitid", "userid"), "left")
      .join(aucDF, Seq("unitid", "userid", "conversion_goal"), "left")
      .select("unitid", "userid", "industry", "put_type", "adslot_type", "conversion_goal",
                                      "cv", "click", "show", "cost", "cpm", "exp_cvr", "pre_cvr",
                                      "post_cvr", "cost_of_every_click", "bid_of_every_click",
                                      "cpa_real", "suggest_cpa", "cpa_given", "hidden_cost_ratio", "kvalue",
                                      "auc", "budget", "cost_budget_ratio")
    val allIndexValueDF = joinIndexValueRingRatio(date, hour, indexValueDF, spark)
    allIndexValueDF
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit("qtt_demo"))
      .repartition(50)
      .write.mode("overwrite").insertInto("test.wt_unit_aa_report")
  }

  // 将统计字段值与环比数据进行join
  def joinIndexValueRingRatio(date: String, hour: String, indexValueDF: DataFrame, spark: SparkSession): DataFrame ={
    indexValueDF.createOrReplaceTempView("index_value_table")
    if("all".equals(hour) == false){
      val sql =
        s"""
          |select
          |    *,
          |    0.0 as cv_ring_ratio,
          |    0.0 as cost_ring_ratio,
          |    0.0 as post_cvr_ring_ratio
          |from
          |    index_value_table
        """.stripMargin
      val allIndexValueDF = spark.sql(sql)
      allIndexValueDF
    }else{
      // 首先获取前一天的数据
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val today = sdf.parse(date)
      val calendar = Calendar.getInstance
      calendar.setTime(today)
      calendar.add(Calendar.DATE, -1)
      val yesterday = calendar.getTime
      val preDate = sdf.format(yesterday)
      val sql1 =
        s"""
           |select
           |    unitid,
           |    userid,
           |    cv,
           |    cost,
           |    post_cvr
           |from
           |    test.wt_unit_aa_report
           |where
           |    `date` = "$preDate"
           |and
           |    hour = 'all'
      """.stripMargin
      println(sql1)
      spark.sql(sql1).createOrReplaceTempView("yesterday_data_table")

      val sql2 =
        s"""
          |select
          |    a.*,
          |    round(a.cv / b.cv, 4) as cv_ring_ratio,
          |    round(a.cost / b.cost, 4) as cost_ring_ratio,
          |    round(a.post_cvr / b.post_cvr) as post_cvr_ring_ratio
          |from
          |    index_value_table a
          |left join
          |    yesterday_data_table b
          |on
          |    a.unitid = b.unitid
          |and
          |    a.userid = b.userid
        """.stripMargin
      println("-----------------")
      println(sql2)
      val allIndexValueDF = spark.sql(sql2)
      allIndexValueDF
    }
  }
}
