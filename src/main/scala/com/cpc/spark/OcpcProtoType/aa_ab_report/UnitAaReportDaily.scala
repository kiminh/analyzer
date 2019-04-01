package com.cpc.spark.OcpcProtoType.aa_ab_report

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object UnitAaReportDaily {
  def main(args: Array[String]): Unit = {

  }

  // 获取统计数据
  def getIndexValue(date: String, spark: SparkSession): Unit ={
    val sql1 =
      s"""
        |select
        |    unitid,
        |    userid,
        |    industry,
        |    round((case when sum(case when isclick is null then 0 else 1 end) = 0 then avg(is_ocpc)
        |                else sum(case when isclick = 1 then is_ocpc else 0 end) * 1.0 / sum(case when isclick is null then 0 else 1 end) end), 4) as put_type,
        |    adslot_type,
        |    conversion_goal,
        |    sum(iscvr) as cv,
        |    sum(isclick) as click,
        |    sum(isshow) as show,
        |    sum(case when isclick = 1 then bid else 0 end) * 0.01 / sum(isclick) as bid,
        |    sum(case when isclick = 1 then price else 0 end) * 0.01 as cost,
        |    round(sum(case when isclick = 1 then price else 0 end) * 10.0 / sum(isshow), 4) as cpm,
        |    sum(case when isclick = 1 then exp_cvr else 0 end) * 1.0 / sum(isclick) as exp_cvr,
        |    round(sum(case when isclick = 1 then pcvr else 0 end) * 1.0 / sum(isclick), 4) as pre_cvr,
        |    round(sum(iscvr) * 1.0 / sum(isclick), 4) as post_cvr,
        |    round(sum(case when isclick = 1 then price else 0 end) * 0.01 / sum(isclick), 4) as cost_of_every_click,
        |    round(sum(case when isclick = 1 then bid else 0 end) * 0.01/ sum(isclick), 4) as bid_of_every_click,
        |    round(sum(case when isclick = 1 then price else 0 end) * 0.01 / sum(iscvr), 4) as cpa_real,
        |    round(sum(case when isclick = 1 then cpa_given else 0 end) * 0.01 / sum(isclick), 4)  as cpa_given,
        |    round(sum(case when isclick = 1 and is_hidden = 1 then price else 0 end)
        |          / sum(case when isclick = 1 then price else 0 end), 4) hidden_cost_ratio,
        |    round(sum(case when isclick = 1 then kvalue else 0 end) * 1.0 / sum(isclick), 4) as kvalue,
        |    max(case when isclick = 1 then budget * 0.01 else 0 end) as budget,
        |    round(case when (max(case when isclick = 1 then budget else 0 end)) = 0 then 0
        |               else (sum(case when isclick = 1 then price else 0 end) * 1.0 / max(case when isclick = 1 then budget else 0 end)) end, 4) as cost_budget_ratio,
        |    round(sum(case when isclick = 1 and is_ocpc = 1 then price else 0 end) * 10.0
        |          / sum(case when isshow= 1 and is_ocpc = 1 then 1 else 0 end), 4) as ocpc_cpm,
        |    round(sum(case when isclick = 1 and is_ocpc != 1 then price else 0 end) * 10.0
        |          / sum(case when isshow= 1 and is_ocpc != 1 then 1 else 0 end), 4) as cpc_cpm,
        |    round(sum(case when isclick = 1 and is_ocpc = 1 then pcvr else 0 end) * 1.0
        |         / sum(case when isclick = 1 and is_ocpc = 1 then 1 else 0 end), 4) as ocpc_pre_cvr,
        |    round(sum(case when isclick = 1 and is_ocpc != 1 then pcvr else 0 end) * 1.0
        |         / sum(case when isclick = 1 and is_ocpc != 1 then 1 else 0 end), 4) as cpc_pre_cvr,
        |    round(sum(case when is_ocpc = 1 then iscvr else 0 end) * 1.0
        |         / sum(case when isclick = 1 and is_ocpc = 1 then 1 else 0 end), 4) as ocpc_post_cvr,
        |    round(sum(case when is_ocpc != 1 then iscvr else 0 end) * 1.0
        |         / sum(case when is_ocpc != 1 and isclick = 1 then 1 else 0 end), 4) as cpc_post_cvr,
        |    round(sum(case when isclick = 1 and is_ocpc = 1 then price else 0 end) * 0.01
        |         / sum(case when isclick = 1 and is_ocpc = 1 then 1 else 0 end), 4) as ocpc_cost_of_every_click,
        |    round(sum(case when isclick = 1 and is_ocpc != 1 then price else 0 end) * 0.01
        |         / sum(case when isclick = 1 and is_ocpc != 1 then 1 else 0 end), 4) as cpc_cost_of_every_click,
        |    round(sum(case when isclick = 1 and is_ocpc = 1 then bid else 0 end) * 0.01
        |         / sum(case when isclick = 1 and is_ocpc = 1 then 1 else 0 end), 4) as ocpc_bid_of_every_click,
        |    round(sum(case when isclick = 1 and is_ocpc != 1 then bid else 0 end) * 0.01
        |         / sum(case when isclick = 1 and is_ocpc != 1 then 1 else 0 end), 4) as cpc_bid_of_every_click,
        |    round(sum(case when isclick = 1 and is_ocpc = 1 then price else 0 end) * 0.01
        |         / sum(case when is_ocpc = 1 then iscvr else 0 end), 4) as ocpc_cpa_real,
        |    round(sum(case when isclick = 1 and is_ocpc != 1 then price else 0 end) * 0.01
        |         / sum(case when is_ocpc != 1 then iscvr else 0 end), 4) as cpc_cpa_real
        |from
        |    dl_cpc.ocpc_aa_ab_report_base_data
        |where
        |    `date` = '$date'
        |and
        |    version = 'qtt_demo'
        |group by
        |    unitid,
        |    userid,
        |    industry,
        |    adslot_type,
        |    conversion_goal
      """.stripMargin
    println("------get index value sql1-------")
    println(sql1)
    spark.sql(sql1).createOrReplaceTempView("temp_table")

    val sql2 =
      s"""
        |select
        |    unitid,
        |    userid,
        |    industry,
        |    put_type,
        |    adslot_type,
        |    conversion_goal,
        |    cv,
        |    click,
        |    show,
        |    bid,
        |    cost,
        |    cpm,
        |    exp_cvr,
        |    pre_cvr,
        |    post_cvr,
        |    cost_of_every_click,
        |    bid_of_every_click,
        |    cpa_real,
        |    cpa_given,
        |    hidden_cost_ratio,
        |    kvalue,
        |    budget,
        |    cost_budget_ratio,
        |    round((case when cpc_cpm != 0 then ocpc_cpm / cpc_cpm else 0 end), 4) as ocpc_cpc_cpm_ratio,
        |    round((case when cpc_pre_cvr != 0 then ocpc_pre_cvr / cpc_pre_cvr else 0 end), 4) as ocpc_cpc_pre_cvr_ratio,
        |    round((case when cpc_post_cvr != 0 then ocpc_post_cvr / cpc_post_cvr else 0 end), 4) as ocpc_cpc_post_cvr_ratio,
        |    round((case when cpc_cost_of_every_click != 0 then ocpc_cost_of_every_click / cpc_cost_of_every_click else 0 end), 4) as ocpc_cpc_cost_of_every_click,
        |    round((case when cpc_bid_of_every_click != 0 then ocpc_bid_of_every_click / cpc_bid_of_every_click else 0 end), 4) as ocpc_cpc_bid_of_every_click,
        |    round((case when cpc_cpa_real != 0 then ocpc_cpa_real / cpc_cpa_real else 0 end), 4) as ocpc_cpc_cpa_real
        |from
        |    temp_table
      """.stripMargin
    println("------get index value sql2-------")
    println(sql2)
    // 创建临时表
    spark.sql(sql2).createOrReplaceTempView("base_index_value_table")
    getSuggestCpa(date, spark).createOrReplaceTempView("suggest_cpa_table")
    getAuc(date, spark).createOrReplaceTempView("auc_table")
    getPreDateData(date, spark).createOrReplaceTempView("yesterday_data_table")

    val sql3 =
      s"""
        |select
        |    a.*,
        |    b.suggest_cpa,
        |    c.auc,
        |    (case when d.cv = 0 then null else round(a.cv / d.cv, 4) end) as cv_ring_ratio,
        |    (case when d.cost = 0 then null else round(a.cost / d.cost, 4) end) as cost_ring_ratio,
        |    (case when d.post_cvr = 0 then null else round(a.post_cvr / d.post_cvr, 4) end) as post_cvr_ring_ratio,
        |    (case when d.cpm = 0 then null else round(a.cpm / d.cpm, 4) end) as cpm_ring_ratio
        |from
        |    base_index_value_table a
        |left join
        |    suggest_cpa_table b
        |on
        |    a.unitid = b.unitid
        |and
        |    a.userid = b.userid
        |left join
        |    auc_table c
        |on
        |    a.unitid = c.unitid
        |and
        |    a.conversion_goal = c.conversion_goal
        |left join
        |    yesterday_data_table d
        |on
        |    a.unitid = d.unitid
        |and
        |    a.userid = d.userid
      """.stripMargin
    println("------get index value sql3-------")
    println(sql3)
    val dataDF = spark.sql(sql3)
                      .withColumn("date", lit(date))
                      .withColumn("version", lit("qtt_demo"))
    // 首先将aa报表写入hive表
//    dataDF
//      .repartition(50)
//      .write.mode("overwrite").insertInto("dl_cpc.ocpc_unit_aa_report_daily")

//    val data = dataDF.rdd.map(x => Seq(x.getAs[Int]("unitid").toString,
//      x.getAs[Int]("userid").toString, x.getAs[String]("industry").toString,
//      x.getAs[Double]("put_type").toString, x.getAs[Int]("adslot_type").toString,
//      x.getAs[Int]("conversion_goal").toString, x.getAs[Int]("cv").toString,
//      x.getAs[Int]("click").toString, x.getAs[Int]("show").toString,
//      x.getAs[Double]("bid").toString, x.getAs[Double]("cost").toString,
//      x.getAs[Double]("cpm").toString, x.getAs[Double]("exp_cvr").toString,
//      x.getAs[Double]("pre_cvr").toString, x.getAs[Double]("post_cvr").toString,x.getAs[Double]("bid").toString, x.getAs[Double]("cost").toString,
//      x.getAs[Double]("cost_of_every_click").toString, x.getAs[Double]("bid_of_every_click").toString,
//      x.getAs[Double]("cpa_real").toString, x.getAs[Double]("cpa_given").toString,
//      x.getAs[Double]("hidden_cost_ratio").toString, x.getAs[Double]("kvalue").toString,
//      x.getAs[Double]("budget").toString, x.getAs[Double]("cost_budget_ratio").toString,
//      x.getAs[Double]("ocpc_cpc_cpm_ratio").toString, x.getAs[Double]("ocpc_cpc_pre_cvr_ratio").toString,
//      x.getAs[Double]("ocpc_cpc_post_cvr_ratio").toString, x.getAs[Double]("ocpc_cpc_cost_of_every_click").toString,
//      x.getAs[Double]("ocpc_cpc_bid_of_every_click").toString, x.getAs[Double]("ocpc_cpc_cpa_real").toString,
//      x.getAs[Double]("suggest_cpa").toString, x.getAs[Double]("auc").toString,
//      x.getAs[Double]("cv_ring_ratio").toString, x.getAs[Double]("cost_ring_ratio").toString,
//      x.getAs[Double]("post_cvr_ring_ratio").toString, x.getAs[Double]("cpm_ring_ratio").toString).mkString(","))
    val t = spark.sparkContext.parallelize(Seq("1111")).map(x => (x,1))

    val title = spark.sparkContext.parallelize(Seq(Seq("unitid", "userid", "industry", "put_type",
                "adslot_type", "conversion_goal", "cv", "click", "show", "bid", "cost", "cpm",
                "exp_cvr", "pre_cvr", "post_cvr", "cost_of_every_click", "bid_of_every_click",
                "cpa_real", "cpa_given", "hidden_cost_ratio", "kvalue", "budget", "cost_budget_ratio",
                "ocpc_cpc_cpm_ratio", "ocpc_cpc_pre_cvr_ratio", "ocpc_cpc_post_cvr_ratio",
                "ocpc_cpc_cost_of_every_click", "ocpc_cpc_bid_of_every_click", "ocpc_cpc_cpa_real",
                "suggest_cpa", "auc", "cv_ring_ratio", "cost_ring_ratio","post_cvr_ring_ratio",
                "cpm_ring_ratio").mkString(",")))
      .map(x => (x, 1))
    val data = title.union(dataDF.rdd.map(x => Seq(x.getAs[Int]("unitid") + "",
      x.getAs[Int]("userid") + "", x.getAs[String]("industry") + "",
      x.getAs[Double]("put_type") + "", x.getAs[Int]("adslot_type") + "",
      x.getAs[Int]("conversion_goal") + "", x.getAs[Long]("cv") + "",
      x.getAs[Long]("click") + "", x.getAs[Long]("show") + "",
      x.getAs[BigDecimal]("bid") + "", x.getAs[BigDecimal]("cost") + "",
      x.getAs[BigDecimal]("cpm") + "", x.getAs[BigDecimal]("exp_cvr") + "",
      x.getAs[BigDecimal]("pre_cvr") + "", x.getAs[BigDecimal]("post_cvr")+ "",
      x.getAs[BigDecimal]("cost_of_every_click") + "", x.getAs[BigDecimal]("bid_of_every_click") + "",
      x.getAs[BigDecimal]("cpa_real")+ "", x.getAs[BigDecimal]("cpa_given") + "",
      x.getAs[BigDecimal]("hidden_cost_ratio") + "", x.getAs[BigDecimal]("kvalue") + "",
      x.getAs[BigDecimal]("budget") + "", x.getAs[BigDecimal]("cost_budget_ratio") + "",
      x.getAs[BigDecimal]("ocpc_cpc_cpm_ratio") + "", x.getAs[BigDecimal]("ocpc_cpc_pre_cvr_ratio") + "",
      x.getAs[BigDecimal]("ocpc_cpc_post_cvr_ratio") + "", x.getAs[BigDecimal]("ocpc_cpc_cost_of_every_click") + "",
      x.getAs[BigDecimal]("ocpc_cpc_bid_of_every_click") + "", x.getAs[BigDecimal]("ocpc_cpc_cpa_real") + "",
      x.getAs[BigDecimal]("suggest_cpa") + "", x.getAs[BigDecimal]("auc") + "",
      x.getAs[BigDecimal]("cv_ring_ratio") + "", x.getAs[BigDecimal]("cost_ring_ratio") + "",
      x.getAs[BigDecimal]("post_cvr_ring_ratio") + "", x.getAs[BigDecimal]("cpm_ring_ratio") + "",
      x.getAs[String]("date")).mkString(",")).map(x => (x, 2)))
        .sortBy(_._2)
        .map(x => x._1)

    data.repartition(1).saveAsTextFile(s"/user/cpc/wentao/unit_aa_report/aa_report_$date")

    /**
      * /www/part-0000
      * hadoop fs -copyFromLocal /www/part-000 a.csv
      * hadoop fs -get hdfs://emr-cluster/user/cpc/wentao/unit_aa_report/aa_report_2019-03-30/part-00000
      */
    // 然后将aa报表写入csv
//    val filePath = s"/home/cpc/wt/aa_report_daily/aa_report_daily_$date"
    val headName = List("unitid", "userid", "industry", "put_type", "adslot_type", "conversion_goal",
      "cv", "click", "show", "bid", "cost", "cpm", "exp_cvr", "pre_cvr", "post_cvr", "cost_of_every_click",
      "bid_of_every_click", "cpa_real", "cpa_given", "hidden_cost_ratio", "kvalue", "budget",
      "cost_budget_ratio", "ocpc_cpc_cpm_ratio", "ocpc_cpc_pre_cvr_ratio", "ocpc_cpc_post_cvr_ratio",
      "ocpc_cpc_cost_of_every_click", "ocpc_cpc_bid_of_every_click", "ocpc_cpc_cpa_real",
      "suggest_cpa", "auc", "cv_ring_ratio", "cost_ring_ratio","post_cvr_ring_ratio", "cpm_ring_ratio")
//    WriteCsv.write(dataDF, headName, filePath)
  }

  // 获得suggest_cpa，只有天级别的
  def getSuggestCpa(date: String, spark: SparkSession): DataFrame ={
    val sql =
      s"""
         |select
         |    a.unitid,
         |    a.userid,
         |    round(a.cpa * 0.01, 4) as suggest_cpa
         |from
         |    (select
         |        unitid,
         |        userid,
         |        cpa,
         |        row_number() over(partition by unitid, userid order by cost desc) as row_num
         |    from
         |        dl_cpc.ocpc_suggest_cpa_recommend_hourly
         |    where
         |       version = 'qtt_demo'
         |    and
         |       `date` = '$date'
         |    and
         |        hour = '06') a
         |where
         |    a.row_num = 1
      """.stripMargin
    println("------get suggest cpa sql-------")
    println(sql)
    val suggestCpaDF = spark.sql(sql)
    suggestCpaDF
  }
  // 获取auc，只有天级别的
  def getAuc(date: String, spark: SparkSession): DataFrame ={
    val sql =
      s"""
         |select
         |    unitid,
         |    auc,
         |    conversion_goal
         |from
         |    dl_cpc.ocpc_unitid_auc_daily
         |where
         |    `date` = '$date'
         |and
         |    version = 'qtt_demo'
      """.stripMargin
    println("------get auc sql-------")
    println(sql)
    val aucDF = spark.sql(sql)
    aucDF
  }

  // 获取前一天的cv、cost、post_cvr、cpm
  def getPreDateData(date: String, spark: SparkSession): DataFrame ={
    // 得到前一天的时间
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val today = sdf.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val yesterday = calendar.getTime
    val preDate = sdf.format(yesterday)

    val sql =
      s"""
        |select
        |    unitid,
        |    userid,
        |    cv,
        |    cost,
        |    post_cvr,
        |    cpm
        |from
        |    dl_cpc.ocpc_unit_aa_report_daily
        |where
        |    `date` = '$preDate'
      """.stripMargin
    println("------get yesterday's data sql-------")
    println(sql)
    val preDateDataDF = spark.sql(sql)
    preDateDataDF
  }

}
