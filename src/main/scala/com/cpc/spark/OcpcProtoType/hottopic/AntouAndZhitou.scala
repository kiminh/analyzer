package com.cpc.spark.OcpcProtoType.hottopic

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit

object AntouAndZhitou {

  def main(args: Array[String]): Unit = {
    val date = args(0).toString
    val spark = SparkSession.builder().appName("AntouAndZhitou").enableHiveSupport().getOrCreate()
    getInfo(date, spark)
    println("----- success -----")
  }

  def getInfo(date: String, sprak: SparkSession): Unit ={
    val costDF = getCost(date, sprak)
    val antouDF = getAntouInfo(date, costDF, sprak)
    val ztfzqDF = getZTFZQOthersInfo(date, costDF, sprak)
    val ztzqfoDF = getZTZQNOInfo(date, costDF, sprak)
    val ztocpcDF = getZTZQOInfo(date, costDF, sprak)
    antouDF.createOrReplaceTempView("temp_table1")
    ztfzqDF.createOrReplaceTempView("temp_table2")
    ztzqfoDF.createOrReplaceTempView("temp_table3")
    ztocpcDF.createOrReplaceTempView("temp_table4")
    val sql =
      s"""
        |select * from temp_table1
        |union
        |select * from temp_table2
        |union
        |select * from temp_table3
        |union
        |select * from temp_table4
      """.stripMargin
    val resultDF = sprak.sql(sql)
    resultDF
      .withColumn("date", lit(date))
      .repartition(2)
      .write.mode("overwrite")
      .insertInto("dl_cpc.hottopic_antou_zhitou_info")
  }

  // 统计hottopic消费和其他类型的消费
  def getCost(date: String, spark: SparkSession): DataFrame ={
    val sql =
      s"""
        |select
        |    unitid,
        |    sum(case when media_appsid = '80002819' then price else 0 end) * 0.01 as hottopic_cost,
        |    sum(case when media_appsid <> '80002819' then price else 0 end) * 0.01 as other_cost
        |from
        |    dl_cpc.ocpc_base_unionlog
        |where
        |    `date` = '$date'
        |and
        |    isshow = 1
        |and
        |    isclick = 1
        |and
        |    antispam = 0
        |and
        |    adsrc = 1
        |and
        |    (charge_type = 1 or charge_type is null)
        |group by
        |    unitid
      """.stripMargin
    println("--------- get cost -----------")
    println(sql)
    val costDF = spark.sql(sql)
    costDF
  }

  // 统计暗投的cpm、ocpc_cost、all_cost等信息
  def getAntouInfo(date: String, costDF: DataFrame, spark: SparkSession): DataFrame ={
    costDF.createOrReplaceTempView("temp_table")
    val sql =
      s"""
        |select
        |    (case when usertype = 0 then 'antou_other'
        |          when usertype = 1 then 'antou_heiwulei'
        |          else 'antou_zhengqi'end) as type,
        |    sum(case when a.isclick = 1 and length(a.ocpc_log) > 0 then a.price else 0 end) * 0.01 as ocpc_cost,
        |    sum(case when length(a.ocpc_log) > 0 then 1 else 0 end) as ocpc_show,
        |    round(sum(case when a.isclick = 1 and length(a.ocpc_log) > 0 then a.price else 0 end) * 10.0
        |        / sum(case when length(a.ocpc_log) > 0 then a.isshow else 0 end), 3) as ocpc_cpm,
        |    sum(case when a.isclick = 1 then a.price else 0 end) * 0.01 as all_cost,
        |    count(a.unitid) as all_show,
        |    round(sum(case when a.isclick = 1 then a.price else 0 end) * 10.0
        |        / sum(a.isshow), 3) as all_cpm,
        |    round(sum(case when a.isclick = 1 and length(a.ocpc_log) > 0 then a.price else 0 end)
        |        / sum(case when a.isclick = 1 then a.price else 0 end), 4) as ratio
        |from
        |    (select
        |        unitid,
        |        isshow,
        |        isclick,
        |        price,
        |        is_ocpc,
        |        usertype,
        |        ocpc_log
        |    from
        |        dl_cpc.ocpc_base_unionlog
        |    where
        |        `date` = '$date'
        |    and
        |        isshow = 1
        |    and
        |        antispam = 0
        |    and
        |        adsrc = 1
        |    and
        |        (charge_type = 1 or charge_type is null)
        |    and
        |        media_appsid = '80002819') a
        |right join
        |    temp_table b
        |on
        |    a.unitid = b.unitid
        |where
        |    b.hottopic_cost > 0
        |and
        |    b.other_cost > 0
        |group by
        |    (case when usertype = 0 then 'other'
        |          when usertype = 1 then 'heiwulei'
        |          else 'zhengqi' end)
      """.stripMargin
    println("--------- get antou info -----------")
    println(sql)
    val antouDF = spark.sql(sql)
    antouDF
  }

  // 统计直投非企、其他的cpm、ocpc_cost、all_cost等信息
  def getZTFZQOthersInfo(date: String, costDF: DataFrame, spark: SparkSession): DataFrame = {
    costDF.createOrReplaceTempView("temp_table")
    val sql =
      s"""
        |select
        |    (case when usertype = 0 then 'zhitou_other' else 'zhitou_heiwulei' end) as type,
        |    sum(case when a.isclick = 1 and a.is_ocpc = 1 then a.price else 0 end) * 0.01 as ocpc_cost,
        |    sum(case when a.is_ocpc = 1 then 1 else 0 end) as ocpc_show,
        |    round(sum(case when a.isclick = 1 and a.is_ocpc = 1 then a.price else 0 end) * 10.0
        |        / sum(case when a.is_ocpc = 1 then 1 else 0 end), 3) as ocpc_cpm,
        |    sum(case when a.isclick = 1 then a.price else 0 end) * 0.01 as all_cost,
        |    sum(a.isshow) as all_show,
        |    round(sum(case when a.isclick = 1 then a.price else 0 end) * 10.0
        |        / sum(a.isshow), 3) as all_cpm,
        |    round(sum(case when a.isclick = 1 and a.is_ocpc = 1 then a.price else 0 end)
        |        / sum(case when a.isclick = 1 then a.price else 0 end), 4) as ratio
        |from
        |    (select
        |        unitid,
        |        isshow,
        |        isclick,
        |        price,
        |        is_ocpc,
        |        usertype,
        |        ocpc_log
        |    from
        |        dl_cpc.ocpc_base_unionlog
        |    where
        |        `date` = '$date'
        |    and
        |        isshow = 1
        |    and
        |        antispam = 0
        |    and
        |        adsrc = 1
        |    and
        |        (charge_type = 1 or charge_type is null)
        |    and
        |        media_appsid = '80002819'
        |    and
        |        usertype in (0, 1)) a
        |right join
        |    temp_table b
        |on
        |    a.unitid = b.unitid
        |where
        |    b.hottopic_cost > 0
        |and
        |    b.other_cost = 0
        |group by
        |    (case when usertype = 0 then 'zhitou_other' else 'zhitou_heiwulei' end)
      """.stripMargin
    println("--------- get zhitou other -----------")
    println(sql)
    val ztfzqDF = spark.sql(sql)
    ztfzqDF
  }

  // 统计直投正企非ocpc的cpm、ocpc_cost、all_cost等信息
  def getZTZQNOInfo(date: String, costDF: DataFrame, spark: SparkSession): DataFrame = {
    costDF.createOrReplaceTempView("temp_table")
    val sql =
      s"""
        |select
        |    'zhitou_zq_not_ocpc' as type,
        |    sum(case when isclick = 1 and is_ocpc = 1 then price else 0 end) * 0.01 as ocpc_cost,
        |    sum(case when is_ocpc = 1 then 1 else 0 end) as ocpc_show,
        |    round(sum(case when isclick = 1 and is_ocpc = 1 then price else 0 end) * 10.0
        |        / sum(case when is_ocpc = 1 then 1 else 0 end), 3) as ocpc_cpm,
        |    sum(case when isclick = 1 then price else 0 end) * 0.01 as all_cost,
        |    sum(isshow) as all_show,
        |    round(sum(case when isclick = 1 then price else 0 end) * 10.0
        |        / sum(isshow), 3) as all_cpm,
        |    round(sum(case when isclick = 1 and is_ocpc = 1 then price else 0 end)
        |        / sum(case when isclick = 1 then price else 0 end), 4) as ratio
        |from
        |    (select
        |        unitid,
        |        isshow,
        |        isclick,
        |        price,
        |        is_ocpc,
        |        usertype,
        |        ocpc_log
        |    from
        |        dl_cpc.ocpc_base_unionlog
        |    where
        |        `date` = '$date'
        |    and
        |        isshow = 1
        |    and
        |        antispam = 0
        |    and
        |        adsrc = 1
        |    and
        |        (charge_type = 1 or charge_type is null)
        |    and
        |        media_appsid = '80002819'
        |    and
        |        usertype = 2
        |    and
        |        is_ocpc = 0) a
        |right join
        |    temp_table b
        |on
        |    a.unitid = b.unitid
        |where
        |    b.hottopic_cost > 0
        |and
        |    b.other_cost = 0
        |group by
        |    'zhitou_zq_not_ocpc'
      """.stripMargin
    println("--------- get zhitou not ocpc -----------")
    println(sql)
    val ztzqfoDF = spark.sql(sql)
    ztzqfoDF
  }

  // 统计直投正企ocpc的cpm、ocpc_cost、all_cost等信息
  def getZTZQOInfo(date: String, costDF: DataFrame, spark: SparkSession): DataFrame = {
    costDF.createOrReplaceTempView("temp_table")
    val sql =
      s"""
        |select
        |    'zhitou_ocpc' as type,
        |    sum(case when isclick = 1 and is_ocpc = 1 then price else 0 end) * 0.01 as ocpc_cost,
        |    sum(case when is_ocpc = 1 then 1 else 0 end) as ocpc_show,
        |    round(sum(case when isclick = 1 and is_ocpc = 1 then price else 0 end) * 10.0
        |        / sum(case when is_ocpc = 1 then 1 else 0 end), 3) as ocpc_cpm,
        |    sum(case when isclick = 1 then price else 0 end) * 0.01 as all_cost,
        |    sum(isshow) as all_show,
        |    round(sum(case when isclick = 1 then price else 0 end) * 10.0
        |        / sum(isshow), 3) as all_cpm,
        |    round(sum(case when isclick = 1 and is_ocpc = 1 then price else 0 end)
        |        / sum(case when isclick = 1 then price else 0 end), 4) as ratio
        |from
        |    (select
        |        unitid,
        |        isshow,
        |        isclick,
        |        price,
        |        is_ocpc,
        |        usertype,
        |        ocpc_log
        |    from
        |        dl_cpc.ocpc_base_unionlog
        |    where
        |        `date` = '$date'
        |    and
        |        isshow = 1
        |    and
        |        antispam = 0
        |    and
        |        adsrc = 1
        |    and
        |        (charge_type = 1 or charge_type is null)
        |    and
        |        media_appsid = '80002819'
        |    and
        |        is_ocpc = 1) a
        |right join
        |    temp_table b
        |on
        |    a.unitid = b.unitid
        |where
        |    b.hottopic_cost > 0
        |and
        |    b.other_cost = 0
        |group by
        |    'zhitou_ocpc'
      """.stripMargin
    println("--------- get zhitou ocpc -----------")
    println(sql)
    val ztocpcDF = spark.sql(sql)
    ztocpcDF
  }
}
