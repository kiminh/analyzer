package com.cpc.spark.OcpcProtoType.report_qtt

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ListBuffer

object OcpcGetDarkTestData {

  def main(args: Array[String]): Unit = {
    val date = args(0).toString
    val jsonPath = "/user/cpc/wangjun/ocpc_cpagivenv2.json"
    val spark = SparkSession.builder().appName("OcpcGetDarkTestData").enableHiveSupport().getOrCreate()
    val (unitids, jsonDF) = parseJson(jsonPath, spark)
    println("has parsed json")
    val indexDF = getBaseIndex(date, unitids, spark)
    println("has got index")
    getData(indexDF, jsonDF, date, spark)
    println("has got all dark test data")
    getTimeInterval(indexDF, date, spark)
    println("has got time interval")
  }

  // 首先获取统计数据需要的基本字段
  def getBaseIndex(date: String, unitids: String, spark: SparkSession): DataFrame ={
    val sql =
      s"""
        |select
        |    a.searchid,
        |    a.exptags,
        |    a.unitid,
        |    a.ideaid,
        |    a.userid,
        |    a.isclick,
        |    a.isshow,
        |    a.price,
        |    a.exp_cvr,
        |    a.is_ocpc,
        |    b.iscvr1,
        |    c.iscvr2,
        |    d.iscvr3,
        |    a.hour
        |from
        |    (select
        |        searchid,
        |        exptags,
        |        ideaid,
        |        unitid,
        |        userid,
        |        isclick,
        |        isshow,
        |        price,
        |        exp_cvr,
        |        is_ocpc,
        |        hour
        |    from
        |        dl_cpc.ocpc_base_unionlog
        |    where
        |        `date` = '$date'
        |    and
        |        media_appsid  in ("80000001", "80000002")
        |    and
        |       unitid in ($unitids)
        |    and
        |        isshow = 1
        |    and
        |        antispam = 0
        |    and
        |        adslot_type in (1,2,3)
        |    and
        |        adsrc = 1
        |    and
        |        (charge_type is null or charge_type = 1)
        |    ) as a
        |left join
        |    (select
        |        searchid,
        |        label2 as iscvr1
        |    from
        |        dl_cpc.ml_cvr_feature_v1
        |    where
        |        `date` >= '$date'
        |    and
        |        label2 = 1
        |    and
        |        label_type in (1, 2, 3, 4, 5)
        |    group by searchid, label2) as b
        |on
        |    a.searchid = b.searchid
        |left join
        |    (select
        |        searchid,
        |        label as iscvr2
        |    from
        |        dl_cpc.ml_cvr_feature_v2
        |    where
        |        `date` >= '$date'
        |    and
        |        label = 1
        |    group by searchid, label) as c
        |on
        |    a.searchid = c.searchid
        |left join
        |    (select
        |        searchid,
        |        1 as iscvr3
        |    from
        |        dl_cpc.site_form_unionlog
        |    where
        |        `date` >= '$date'
        |    and
        |        ideaid > 0
        |    and
        |        searchid is not null
        |    group by searchid) as d
        |on
        |    a.searchid = d.searchid
      """.stripMargin
    val indexDF = spark.sql(sql)
    indexDF
  }

  // 从json配置文件中获得unitid以及它对应的conversion_goal
  def parseJson(jsonPath: String, spark: SparkSession) ={
    val jsonDF = spark.read.format("json").json(jsonPath)
    // 获得unitid
    val unitidDF = jsonDF.select("identifier").collect()
    val unitidList = new ListBuffer[String]
    for(id <- unitidDF){
      unitidList.append(id.getAs[String]("identifier"))
    }
    val unitids = unitidList.mkString(",")
    (unitids, jsonDF)
  }

  // 将两个DateFrame按照unitid进行连接，并统计数据
  def getData(indexDF: DataFrame, jsonDF: DataFrame, date: String, spark: SparkSession): Unit ={
    val allIndexDF = indexDF.join(jsonDF, indexDF("unitid") === jsonDF("identifier"), "left_outer")
    allIndexDF.createOrReplaceTempView("all_index")
    val sql =
      """
        |select
        |    unitid,
        |    userid,
        |    (case when is_ocpc=1 then "ocpc" else "cpc" end) as ab_group,
        |    round(sum(case when isclick=1 then price else 0 end) * 0.01 / sum(isclick), 4) as acp,
        |    round(sum(case when isclick=1 then price else 0 end) * 0.1 / sum(isshow), 4) as cpm,
        |    cpa_given * 0.01 as cpagiven,
        |    round(sum(case when isclick=1 then price else 0 end) * 0.01
        |    / sum(case when conversion_goal = 1 then iscvr1
        |               when conversion_goal = 2 then iscvr2
        |               else iscvr3 end), 4) as cpareal,
        |    round(sum(case when isclick=1 then exp_cvr else 0 end) * 1.0 / sum(isclick), 4) as pre_cvr,
        |    round(sum(case when conversion_goal = 1 then iscvr1
        |                   when conversion_goal = 2 then iscvr2
        |                   else iscvr3 end) * 1.0 / sum(isclick), 4) as post_cvr,
        |    round(sum(case when isclick=1 then price else 0 end) * 0.01, 4) as cost,
        |    sum(isshow) as show,
        |    sum(isclick) as click,
        |    sum(case when conversion_goal = 1 then iscvr1
        |             when conversion_goal = 2 then iscvr2
        |             else iscvr3 end) as cv
        |from
        |    all_index
        |where
        |    `hour` >= '18'
        |group by
        |    unitid,
        |    userid,
        |    (case when is_ocpc=1 then "ocpc" else "cpc" end),
        |    cpa_given
      """.stripMargin
    val data = spark.sql(sql)
    data
      .withColumn("date", lit(date))
      .withColumn("version", lit("qtt_demo"))
      .repartition(100)
      .write.mode("overwrite")
      .insertInto("dl_cpc.ocpc_dark_test_data")
  }

  // 获得每天的投放时间区间
  def getTimeInterval(indexDF: DataFrame, date: String, spark: SparkSession): Unit ={
    indexDF.createOrReplaceTempView("temp_view")
    val sql =
      s"""
        |select
        |	unitid,
        |	hour
        |from
        |	temp_view
        |group by
        |	unitid,
        |	hour
        |order by
        |	unitid,
        |	hour
      """.stripMargin
    val data = spark.sql(sql)
    data
      .withColumn("date", lit(date))
      .withColumn("version", lit("qtt_demo"))
      .repartition(2)
      .write.mode("overwrite")
      .insertInto("dl_cpc.ocpc_dark_test_time")
  }

}
