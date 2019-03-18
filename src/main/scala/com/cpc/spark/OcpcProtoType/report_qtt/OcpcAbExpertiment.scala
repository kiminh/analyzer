package com.cpc.spark.OcpcProtoType.report_qtt

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.typesafe.config.ConfigFactory
import scala.collection.mutable.ListBuffer

object OcpcAbExpertiment {

  def main(args: Array[String]): Unit = {

    val date = args(0).toString
    val tag = args(1).toString
    val spark = SparkSession.builder().appName(name = s"CreateTempTable").enableHiveSupport().getOrCreate()
    val unitids = getUnitid(spark)
    val indexValueDF = createTempTable(tag, date, spark)
    println("has got index value")
    getData(indexValueDF, tag, unitids, date, spark)
    println("has got need data")
    getTime(indexValueDF, tag, unitids, date, spark)
    println("has got time")
  }

  // 获取unitid
  def getUnitid(spark: SparkSession) ={
    // 首先获得json文件的绝对路径
    val conf = ConfigFactory.load("ocpc")
    val expDataPath = conf.getString("ocpc_all.ocpc_cpcbid.path_v2")

    // 然后解析json文件获得unitid
    val jsonData = spark.read.format("json").json(expDataPath)
    val unitidDF = jsonData.select("identifier").distinct().collect()
    var unitidList = new ListBuffer[String]()
    for (id <- unitidDF){
      unitidList.append(id.getAs[String]("identifier"))
    }
    val unitids = unitidList.mkString(",")
    unitids
  }

  // 创建临时表
  def createTempTable(tag: String, date: String, spark: SparkSession): DataFrame = {
    val createTableSQL =
      s"""
        |select
        |    a.`date`,
        |    a.searchid,
        |    a.exptags,
        |    a.unitid,
        |    a.ideaid,
        |    a.userid,
        |    a.isclick,
        |    a.isshow,
        |    a.price,
        |    a.cpagiven,
        |    a.kvalue,
        |    a.pcvr,
        |    a.dynamicbidmax,
        |    a.dynamicbid,
        |    a.conversion_goal,
        |    b.iscvr1,
        |    c.iscvr2,
        |    d.iscvr3,
        |    (case when a.conversion_goal = 1 then b.iscvr1
        |          when a.conversion_goal = 2 then c.iscvr2
        |          else d.iscvr3 end) as iscvr,
        |    hour as hr
        |from
        |    (select
        |        `date`,
        |        searchid,
        |        exptags,
        |        ideaid,
        |        unitid,
        |        userid,
        |        isclick,
        |        isshow,
        |        price,
        |        cast(ocpc_log_dict['cpagiven'] as double) as cpagiven,
        |        cast(ocpc_log_dict['kvalue'] as double) as kvalue,
        |        cast(ocpc_log_dict['pcvr'] as double) as pcvr,
        |        cast(ocpc_log_dict['dynamicbidmax'] as double) as dynamicbidmax,
        |        cast(ocpc_log_dict['dynamicbid'] as double) as dynamicbid,
        |        cast(ocpc_log_dict['conversiongoal'] as int) as conversion_goal,
        |        hour
        |    from
        |        dl_cpc.ocpc_filter_unionlog
        |    where
        |        `date` >= '$date'
        |    and
        |        is_ocpc = 1
        |    and
        |        media_appsid  in ("80000001", "80000002")
        |    and
        |        isshow = 1
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
    val indexValueDF = spark.sql(createTableSQL)
    indexValueDF
  }

  // 抽取数据
  def getData(indexValueDF: DataFrame, tag: String, unitids: String,
              date: String, spark: SparkSession): Unit = {
    indexValueDF.createOrReplaceTempView("temp_index_value")
    val getDataSQL =
      s"""
        |select
        |    `date` as dt,
        |    unitid,
        |    userid,
        |    (case when exptags not like "%,cpcBid%" and exptags not like "%cpcBid,%" then "ocpc" else "cpc" end) as ab_group,
        |    round(sum(case when isclick = 1 then price else 0 end) * 0.01 / sum(isclick), 4) as acp,
        |    round(sum(case when isclick = 1 then dynamicbid else 0 end) * 0.01 / sum(isclick), 4) as acb,
        |    round(sum(case when isclick = 1 then dynamicbidmax else 0 end) * 0.01 / sum(isclick), 4) as acb_max,
        |    round(sum(case when isclick = 1 then price else 0 end)*0.1/sum(isshow), 4) as cpm,
        |    round(sum(case when isclick = 1 then cpagiven else 0 end) * 0.01 / sum(isclick), 4) as cpagiven,
        |    round(sum(case when isclick = 1 then price else 0 end) * 0.01 / sum(iscvr), 4) as cpareal,
        |    round(sum(case when isclick = 1 then pcvr else 0 end) * 1.0 / sum(isclick), 4) as pre_cvr,
        |    round(sum(iscvr) * 1.0 / sum(isclick), 4) as post_cvr,
        |    round(sum(case when isclick = 1 then kvalue else 0 end) * 1.0 / sum(isclick), 4) as kvalue,
        |    round(sum(case when isclick = 1 then price else 0 end) * 0.01, 4) as cost,
        |    sum(isshow) as show,
        |    sum(isclick) as click,
        |    sum(iscvr) as cv
        |from
        |    temp_index_value
        |where
        |    unitid in ($unitids)
        |group by
        |    `date`,
        |    unitid,
        |    userid,
        |    (case when exptags not like "%,cpcBid%" and exptags not like "%cpcBid,%" then "ocpc" else "cpc" end)
      """.stripMargin
    val data = spark.sql(getDataSQL)
    data
        .withColumn("date", lit(date))
        .withColumn("tag", lit(tag))
        .withColumn("version", lit("qtt_demo"))
        .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_ab_test_data")
  }

  // 统计时间
  def getTime(indexValueDF: DataFrame, tag: String, unitids: String,
              date: String, spark: SparkSession): Unit = {
    indexValueDF.createOrReplaceTempView("temp_index_value")
    val getTimeSQL =
      s"""
        |selct
        |    unitid,
        |    `hr`
        |from
        |    temp_index_value
        |where
        |    unitid in ($unitids)
        |group by
            unitid,
            `hr`
        |order by
        |	  unitid,
        |   `hr`
      """.stripMargin
    val time = spark.sql(getTimeSQL)
    time
      .withColumn("date", lit(date))
      .withColumn("tag", lit(tag))
      .withColumn("version", lit("qtt_demo"))
      .repartition(2).write.mode("overwrite").insertInto("dl_cpc.ocpc_ab_test_time")
  }

}
