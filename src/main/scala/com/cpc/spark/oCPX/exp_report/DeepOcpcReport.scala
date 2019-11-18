package com.cpc.spark.oCPX.exp_report

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.deepOcpc.DeepOcpcTools.getTimeRangeSqlDate
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object DeepOcpcReport {
  def main(args: Array[String]): Unit = {
    /*
    次留实验报表数据
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    // bash: 2019-01-02 12
    val date = args(0).toString
    val dayInt = args(1).toInt
    println(s"parameter: date=$date, dayInt=$dayInt")

    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -7)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)

    val rawData = getCompleteExp(date, dayInt, spark)
    rawData
      .select("cali_tag", "recall_tag", "cpa_check_priority", "media", "unitid", "conversion_goal", "deep_conversion_goal", "click", "cost", "pre_cvr1", "pre_cvr2", "cv1", "cv2", "cpagiven", "deep_cpagiven", "date")
      .repartition(1)
      .write.mode("overwrite").insertInto("dl_cpc.deep_ocpc_exp_report_daily")
//      .write.mode("overwrite").insertInto("test.deep_ocpc_exp_report_daily")


  }

  def getCompleteExp(date: String, dayInt: Int, spark: SparkSession) = {
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -7)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)

    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias.total.media_selection"
    val mediaSelection = conf.getString(conf_key)

    val sqlRequest =
      s"""
         |SELECT
         |    cali_tag,
         |    recall_tag,
         |    cpa_check_priority,
         |    media,
         |    unitid,
         |    conversion_goal,
         |    deep_conversion_goal,
         |    date,
         |    sum(isclick) as click,
         |    sum(case when isclick=1 then price else 0 end) *0.01 as cost,
         |    sum(case when isclick=1 then exp_cvr else 0 end) * 1.0 / (1.0 * sum(isclick)) as pre_cvr1,
         |    sum(case when isclick=1 then deep_cvr else 0 end) * 1.0 / (1000000 * sum(isclick)) as pre_cvr2,
         |    sum(iscvr1) as cv1,
         |    sum(iscvr2) as cv2,
         |    sum(case when isclick=1 then cpagiven else 0 end) * 0.01 / sum(isclick) as cpagiven,
         |    sum(case when isclick=1 then deep_cpagiven else 0 end) * 0.01 / sum(isclick) as deep_cpagiven
         |FROM
         |    (select
         |        a.searchid,
         |        a.unitid,
         |        a.conversion_goal,
         |        a.deep_conversion_goal,
         |        a.isclick,
         |        a.deep_cvr,
         |        a.exp_cvr,
         |        a.price,
         |        a.cpa_check_priority,
         |        b.iscvr2,
         |        c.iscvr1,
         |        cast(ocpc_log_dict['cpagiven'] as double) as cpagiven,
         |        cast(deep_cpa as double) as deep_cpagiven,
         |        (case when exptags like '%deepOcpcExpTag:daily%' then 'daily'
         |              when exptags like '%deepOcpcExpTag:v2%' then 'v2'
         |              when exptags like '%deepOcpcExpTag:v3%' then 'v3'
         |              when exptags like '%deepOcpcExpTag:v4%' then 'v4'
         |              else 'dz'
         |        end) as cali_tag,
         |        (case when exptags like '%ocpc_rcl:v205%' then 'v205'
         |              else 'dz'
         |        end) as recall_tag,
         |        (case
         |            when media_appsid in ('80000001', '80000002') then 'qtt'
         |            when media_appsid in ('80002819', '80004944', '80004948') then 'hottopic'
         |            else 'novel'
         |        end) as media,
         |        date
         |    FROM
         |        (select
         |            *
         |        FROM
         |            dl_cpc.ocpc_filter_unionlog
         |        WHERE
         |            date between '${date1}' and '${date}'
         |        and deep_cvr_model_name is not NULL
         |        and $mediaSelection
         |        and is_deep_ocpc = 1) as a
         |    left join
         |        (select
         |            searchid,
         |            deep_conversion_goal,
         |            label as iscvr2
         |        from
         |            dl_cpc.ocpc_label_deep_cvr_hourly
         |        where
         |            date >= '${date1}'
         |        group by searchid, deep_conversion_goal, label) as b
         |    ON
         |        a.searchid = b.searchid
         |    AND
         |        a.deep_conversion_goal = b.deep_conversion_goal
         |    left join
         |        (SELECT
         |            searchid,
         |            (case when cvr_goal = 'cvr1' then 1
         |                when cvr_goal = 'cvr2' then 2
         |                when cvr_goal = 'cvr3' then 3
         |                when cvr_goal = 'cvr4' then 4
         |                else 0
         |            end) as conversion_goal,
         |            label as iscvr1
         |        from
         |            dl_cpc.ocpc_label_cvr_hourly
         |        where
         |            date >= '${date1}'
         |        group by
         |            searchid,
         |            (case when cvr_goal = 'cvr1' then 1
         |                when cvr_goal = 'cvr2' then 2
         |                when cvr_goal = 'cvr3' then 3
         |                when cvr_goal = 'cvr4' then 4
         |                else 0
         |            end),
         |            label) as c
         |    ON
         |        a.searchid = c.searchid
         |    AND
         |        a.conversion_goal = c.conversion_goal) as t
         |group by t.cali_tag, t.recall_tag, t.cpa_check_priority, t.media, t.unitid, t.conversion_goal, t.deep_conversion_goal, t.date
         |""".stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    data
  }

}