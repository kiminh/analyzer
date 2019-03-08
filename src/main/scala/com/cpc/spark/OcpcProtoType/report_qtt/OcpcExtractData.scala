package com.cpc.spark.OcpcProtoType.report_qtt

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory
import scala.collection.mutable.ListBuffer

object OcpcExtractData {

  def main(args: Array[String]): Unit = {

    val date = args(0).toString
    val tag = args(1).toString
    val spark = SparkSession.builder().appName(name = s"CreateTempTable").enableHiveSupport().getOrCreate()
    val unitids = getUnitid(spark)
    createTempTable(tag, date, spark)
    getData(tag, unitids, date, spark)
    getTime(tag, unitids, date, spark)
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
  def createTempTable(tag: String, date: String, spark: SparkSession): Unit = {
    val createTableSQL =
      s"""
        |SELECT
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
        |    date as dt,
        |    hour as hr
        |FROM
        |    (SELECT
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
        |        date,
        |        hour
        |    FROM
        |        dl_cpc.ocpc_filter_unionlog
        |    WHERE
        |        `date` = '$date'
        |    AND
        |        is_ocpc=1
        |    AND
        |        media_appsid  in ("80000001", "80000002")
        |    AND
        |        isshow=1
        |    ) as a
        |LEFT JOIN
        |    (SELECT
        |        searchid,
        |        label2 as iscvr1
        |    FROM
        |        dl_cpc.ml_cvr_feature_v1
        |    WHERE
        |        `date` >= '$date'
        |    AND
        |        label2 = 1
        |    AND
        |        label_type in (1, 2, 3, 4, 5)
        |    GROUP BY searchid, label2) as b
        |ON
        |    a.searchid=b.searchid
        |LEFT JOIN
        |    (SELECT
        |        searchid,
        |        label as iscvr2
        |    FROM
        |        dl_cpc.ml_cvr_feature_v2
        |    WHERE
        |        `date` >= '$date'
        |    AND
        |        label=1
        |    GROUP BY searchid, label) as c
        |ON
        |    a.searchid=c.searchid
        |LEFT JOIN
        |    (SELECT
        |        searchid,
        |        1 as iscvr3
        |    FROM
        |        dl_cpc.site_form_unionlog
        |    WHERE
        |        `date` >= '$date'
        |    AND
        |        ideaid>0
        |    AND
        |        searchid is not NULL
        |    GROUP BY searchid) as d
        |ON
        |    a.searchid=d.searchid
      """.stripMargin
    val data = spark.sql(createTableSQL)
    data
      .withColumn("date", lit(date))
      .withColumn("tag", lit(tag))
      .withColumn("version", lit("qtt_demo"))
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_ab_test_temp")
  }

  // 抽取数据
  def getData(tag: String, unitids: String, date: String, spark: SparkSession): Unit = {
    val getDataSQL =
      s"""
        |SELECT
        |    dt,
        |    unitid,
        |    userid,
        |    (case when exptags not like "%,cpcBid%" and exptags not like "%cpcBid,%" then "ocpc" else "cpc" end) as ab_group,
        |    sum(case when isclick=1 then price else 0 end) * 0.01 / sum(isclick) as acp,
        |    sum(case when isclick=1 then dynamicbid else 0 end) * 0.01 / sum(isclick) as acb,
        |    sum(case when isclick=1 then dynamicbidmax else 0 end) * 0.01 / sum(isclick) as acb_max,
        |    round(sum(case WHEN isclick=1 then price else 0 end)*0.1/sum(isshow),3) as cpm,
        |    sum(case when isclick=1 then cpagiven else 0 end) * 0.01 / sum(isclick) as cpagiven,
        |    sum(case when isclick=1 then price else 0 end) * 0.01 / sum(iscvr) as cpareal,
        |    sum(case when isclick=1 then pcvr else 0 end) * 1.0 / sum(isclick) as pre_cvr,
        |    sum(iscvr) * 1.0 / sum(isclick) as post_cvr,
        |    sum(case when isclick=1 then kvalue else 0 end) * 1.0 / sum(isclick) as kvalue,
        |    sum(case when isclick=1 then price else 0 end) * 0.01 as cost,
        |    sum(isshow) as show,
        |    sum(isclick) as click,
        |    sum(iscvr) as cv
        |FROM
        |    dl_cpc.ocpc_ab_test_temp
        |WHERE
        |    unitid in ($unitids)
        |GROUP BY
        |    dt,
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
  def getTime(tag: String, unitids: String, date: String, spark: SparkSession): Unit = {
    val getTimeSQL =
      s"""
        |SELECT
        |    unitid, `hr`
        |FROM
        |    dl_cpc.ocpc_ab_test_temp
        |WHERE
        |    `date` = '$date'
        |AND
        |    unitid in ($unitids)
        |GROUP BY
            unitid, `hr`
        |ORDER BY
        |	  unitid, `hr`
      """.stripMargin
    val time = spark.sql(getTimeSQL)
    time
      .withColumn("date", lit(date))
      .withColumn("tag", lit(tag))
      .withColumn("version", lit("qtt_demo"))
      .repartition(2).write.mode("overwrite").insertInto("dl_cpc.ocpc_ab_test_time")
  }

}
