package com.cpc.spark.ocpc

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SparkSession
import com.cpc.spark.common.Utils._
import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import com.cpc.spark.udfs.Udfs_wj._
import org.apache.spark.sql.functions._

object OcpcMonitor {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("OcpcMonitor").enableHiveSupport().getOrCreate()

    val date = args(0).toString
    val hour = args(1).toString

    getBaseData(date, hour, spark)
//    getHoursReport(date, hour, spark)
//    getHoursReportWithAct(date, hour, spark)


  }

  def getBaseData(date: String, hour: String, spark: SparkSession): Unit = {
    // 按小时提取所有跑ocpc第二阶段广告的数据记录

    val sqlRequest =
      s"""
         |SELECT
         |    a.uid,
         |    a.timestamp,
         |    a.searchid,
         |    a.exp_ctr,
         |    a.exp_cvr,
         |    a.isclick,
         |    a.isshow,
         |    a.ideaid,
         |    a.adclass,
         |    a.exptags,
         |    a.price,
         |    a.bid_ocpc,
         |    a.ocpc_log,
         |    b.iscvr,
         |    '$date' as date,
         |    '$hour' as hour
         |FROM
         |    (select
         |        uid,
         |        timestamp,
         |        searchid,
         |        userid,
         |        ext['exp_ctr'].int_value * 1.0 / 1000000 as exp_ctr,
         |        ext['exp_cvr'].int_value * 1.0 / 1000000 as exp_cvr,
         |        isclick,
         |        isshow,
         |        ideaid,
         |        ext['adclass'].int_value as adclass,
         |        exptags,
         |        price,
         |        ext_int['bid_ocpc'] as bid_ocpc,
         |        ext_int['is_ocpc'] as is_ocpc,
         |        ext_string['ocpc_log'] as ocpc_log
         |    from
         |        dl_cpc.cpc_union_log
         |    WHERE
         |        `date` = '$date'
         |    and
         |        `hour` = '$hour'
         |    and
         |        media_appsid  in ("80000001", "80000002")
         |    and
         |        ext['antispam'].int_value = 0
         |    and adsrc = 1
         |    and adslot_type in (1,2,3)
         |    and round(ext["adclass"].int_value/1000) != 132101  --去掉互动导流
         |    AND ext_int['is_ocpc']=1) a
         |left outer join
         |    (
         |        select
         |            searchid,
         |            label2 as iscvr
         |        from dl_cpc.ml_cvr_feature_v1
         |        WHERE
         |            `date` = '$date'
         |        and
         |            `hour` = '$hour'
         |        and
         |            label_type!=12
         |    ) b on a.searchid = b.searchid
       """.stripMargin

    println(sqlRequest)

    val dataDF = spark.sql(sqlRequest)

    dataDF.show(10)

    dataDF.write.mode("append").insertInto("dl_cpc.ocpc_result_unionlog_table_bak")
  }

  def getHoursReport(date: String, hour: String, spark: SparkSession): Unit = {
    // 生成前六小时所有跑ocpc第二阶段广告的相关数据指标

    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val endTime = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(endTime)
    calendar.add(Calendar.HOUR, -6)
    val startTime = calendar.getTime
    val tmpDate = dateConverter.format(startTime)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql(date1, hour1, date, hour)


    val sqlRequest =
      s"""
         |SELECT
         |    (case
         |        when exptags like "%ocpc_strategy:2%" then "v2"
         |        else "v1"
         |    end) as model,
         |    ideaid,
         |    AVG(bid_ocpc) as cpa_given,
         |    sum(case WHEN isclick == 1 then price else 0 end)/sum(iscvr) as cpa_real,
         |    sum(CASE WHEN isclick=1 then exp_cvr else 0 end) * 1.0/SUM(isclick) as pcvr,
         |    round(sum(case WHEN isclick == 1 then price else 0 end)*10/sum(isshow),3) as cpm,
         |    round(sum(case WHEN isclick == 1 then price else 0 end)*10/count(distinct uid),3) as arpu,
         |    sum(isclick) * 1.0 / sum(isshow) as ctr,
         |    sum(iscvr) * 1.0 / sum(isclick) as click_cvr,
         |    sum(iscvr) * 1.0 / sum(isshow) as show_cvr,
         |    AVG(price) as price,
         |    COUNT(isshow) as show_cnt,
         |    SUM(isclick) as ctr_cnt,
         |    SUM(iscvr) as cvr_cnt,
         |    '$date' as date,
         |    '$hour' as hour
         |FROM
         |    dl_cpc.ocpc_result_unionlog_table_bak
         |WHERE
         |    $selectCondition
         |GROUP BY
         |    (case
         |        when exptags like "%ocpc_strategy:2%" then "v2"
         |        else "v1"
         |    end),
         |    ideaid
       """.stripMargin
    println(sqlRequest)

    val data = spark.sql(sqlRequest)

    data.show(10)

    //    data.write.mode("overwrite").saveAsTable("test.ocpc_hourly_performance_report")

    data.write.mode("overwrite").insertInto("dl_cpc.ocpc_hourly_performance_report")

  }


  def getHoursReportWithAct(date: String, hour: String, spark: SparkSession) = {

    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -4)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)
    // 抽取基础数据：所有跑ocpc的广告主
    val sqlRequest1 =
      s"""
         |SELECT
         |    a.*,
         |    b.iscvr
         |FROM
         |    (select
         |        uid,
         |        timestamp,
         |        searchid,
         |        userid,
         |        ext['exp_ctr'].int_value * 1.0 / 1000000 as exp_ctr,
         |        ext['exp_cvr'].int_value * 1.0 / 1000000 as exp_cvr,
         |        isclick,
         |        isshow,
         |        ideaid,
         |        exptags,
         |        price,
         |        ext_int['bid_ocpc'] as bid_ocpc,
         |        ext_int['is_ocpc'] as is_ocpc,
         |        ext_string['ocpc_log'] as ocpc_log,
         |        ext_int['is_api_callback'] as is_api_callback,
         |        date,
         |        hour
         |    from
         |        dl_cpc.cpc_union_log
         |    WHERE
         |        $selectCondition
         |    and
         |        media_appsid  in ("80000001", "80000002")
         |    and
         |        ext['antispam'].int_value = 0
         |    and adsrc = 1
         |    and adslot_type in (1,2,3)
         |    and round(ext["adclass"].int_value/1000) != 132101  --去掉互动导流
         |    AND ext_int['is_ocpc']=1
         |    and ext_string['ocpc_log'] != '' and ext_string['ocpc_log'] is not null) a
         |left outer join
         |    (
         |        select
         |            searchid,
         |            label2 as iscvr
         |        from dl_cpc.ml_cvr_feature_v1
         |        WHERE $selectCondition and label_type!=12
         |    ) b on a.searchid = b.searchid
       """.stripMargin
    println(sqlRequest1)
    val rawDataLabel2 = spark.sql(sqlRequest1)
    rawDataLabel2.write.mode("overwrite").saveAsTable("test.ocpc_hourly_complete_data")

    val sqlRequest2 =
      s"""
         |SELECT
         |    ideaid,
         |    SUM(iscvr) as cvr_cnt
         |FROM
         |    dl_cpc.cpc_api_union_log
         |WHERE
         |    $selectCondition
         |AND ext_int['is_ocpc']=1
         |and ext_string['ocpc_log'] != ''
         |and ext_string['ocpc_log'] is not null
         |GROUP BY ideaid
       """.stripMargin
    println(sqlRequest2)
    val label3Data = spark.sql(sqlRequest2)
    label3Data.write.mode("overwrite").saveAsTable("test.ocpc_label3_hourly_data")

    val ocpcAd = rawDataLabel2
      .select("ideaid", "is_api_callback")
      .distinct()
    ocpcAd.write.mode("overwrite").saveAsTable("test.ocpc_hourly_ad_list")

    val sqlRequest3 =
      s"""
         |SELECT
         |    ideaid,
         |    SUM(case when isclick==1 then bid_ocpc else 0 end) * 1.0 / sum(isclick) as cpa_given,
         |    SUM(case when isclick==1 then price else 0 end) as cost,
         |    sum(CASE WHEN isclick=1 then exp_cvr else 0 end) * 1.0/SUM(isclick) as pcvr,
         |    SUM(isshow) as show_cnt,
         |    SUM(isclick) as ctr_cnt,
         |    SUM(iscvr) as cvr_cnt
         |FROM
         |    test.ocpc_hourly_complete_data
         |GROUP BY ideaid
       """.stripMargin
    println(sqlRequest3)
    val label2Data = spark.sql(sqlRequest3)
    label2Data.write.mode("overwrite").saveAsTable("test.ocpc_label2_hourly_data")

    // 计算汇总数据
    val sqlRequest4 =
      s"""
         |SELECT
         |    a.ideaid,
         |    b.cpa_given,
         |    b.cost * 1.0 / b.cvr_cnt as cpa_real,
         |    b.pcvr,
         |    b.ctr_cnt * 1.0 / b.show_cnt as ctr,
         |    b.cvr_cnt * 1.0 / b.ctr_cnt as click_cvr,
         |    b.cvr_cnt * 1.0 / b.show_cnt as show_cvr,
         |    b.cost * 1.0 / b.ctr_cnt as price,
         |    b.show_cnt,
         |    b.ctr_cnt,
         |    b.cvr_cnt,
         |    '$date' as date,
         |    '$hour' as hour
         |FROM
         |    (SELECT
         |        ideaid
         |    FROM
         |        test.ocpc_hourly_ad_list
         |    WHERE
         |        is_api_callback!=1) as a
         |INNER JOIN
         |    test.ocpc_label2_hourly_data as b
         |ON
         |    a.ideaid=b.ideaid
       """.stripMargin
    println(sqlRequest4)
    val noApiData = spark.sql(sqlRequest4)

    val sqlRequest5 =
      s"""
         |SELECT
         |    a.ideaid,
         |    b.cpa_given,
         |    b.cost * 1.0 / c.cvr_cnt as cpa_real,
         |    b.pcvr,
         |    b.ctr_cnt * 1.0 / b.show_cnt as ctr,
         |    c.cvr_cnt * 1.0 / b.ctr_cnt as click_cvr,
         |    c.cvr_cnt * 1.0 / b.show_cnt as show_cvr,
         |    b.cost * 1.0 / b.ctr_cnt as price,
         |    b.show_cnt,
         |    b.ctr_cnt,
         |    c.cvr_cnt,
         |    '$date' as date,
         |    '$hour' as hour
         |FROM
         |    (SELECT
         |        ideaid
         |    FROM
         |        test.ocpc_hourly_ad_list
         |    WHERE
         |        is_api_callback=1) as a
         |INNER JOIN
         |    test.ocpc_label2_hourly_data as b
         |ON
         |    a.ideaid=b.ideaid
         |INNER JOIN
         |    test.ocpc_label3_hourly_data as c
         |ON
         |    a.ideaid=c.ideaid
       """.stripMargin
    println(sqlRequest5)
    val apiData = spark.sql(sqlRequest5)

//    noApiData.write.mode("overwrite").insertInto("dl_cpc.ocpc_check_hourly_report_noapi")
//    apiData.write.mode("overwrite").insertInto("dl_cpc.ocpc_check_hourly_report_api")

    noApiData.write.mode("overwrite").insertInto("dl_cpc.ocpc_check_hourly_report_noapi")
    apiData.write.mode("overwrite").insertInto("dl_cpc.ocpc_check_hourly_report_api")

  }

}