package com.cpc.spark.ocpc

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SparkSession
import com.cpc.spark.common.Utils._
import com.cpc.spark.udfs.Udfs_wj._
import org.apache.spark.sql.functions._

object OcpcMonitor {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("OcpcMonitor").enableHiveSupport().getOrCreate()

    val date = args(0).toString
    val hour = args(1).toString

    getBaseData(date, hour, spark)
    getHoursReport(date, hour, spark)



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
         |    AND ext_int['is_ocpc']=1
         |    AND ext_string['ocpc_log'] != ''
         |    AND ext_string['ocpc_log'] is not null) a
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
//    val selectCondition1 = s"`date`='$date1' and `hour` >= '$hour1'"
//    val selectCondition2 = s"`date`='$date' and `hour`<='$hour'"

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

}