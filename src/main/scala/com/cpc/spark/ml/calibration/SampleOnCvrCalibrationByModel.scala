package com.cpc.spark.ml.calibration

import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.cpc.spark.ocpc.OcpcUtils.{getTimeRangeSql,getTimeRangeSql4}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
import scala.sys.process._

object SampleOnCvrCalibrationByModel {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val endDate = args(0)
    val endHour = args(1)
    val hourRange = args(2).toInt
    val model = args(3)
    val calimodel = args(4)
    val media = args(5)

    val endTime = LocalDateTime.parse(s"$endDate-$endHour", DateTimeFormatter.ofPattern("yyyy-MM-dd-HH"))
    val startTime = endTime.minusHours(Math.max(hourRange - 1, 0))
    val startDate = startTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val startHour = startTime.format(DateTimeFormatter.ofPattern("HH"))

    println(s"endDate=$endDate")
    println(s"endHour=$endHour")
    println(s"hourRange=$hourRange")
    println(s"startDate=$startDate")
    println(s"startHour=$startHour")
    val selectCondition1 = getTimeRangeSql4(startDate, startHour, endDate, endHour)
    val selectCondition2 = getTimeRangeSql(startDate, startHour, endDate, endHour)
    var mediaCondition = "media_appsid in ('80000001','80000002','80000006','80000064','80000066')"
    if (media == "novel"){
      mediaCondition = "media_appsid in ('80001011','80001098','80001292','80001539','80002480','80004786','80004787')"
    } else if (media == "rddz"){
      mediaCondition = "media_appsid in ('80004948','80002819','80004944','80004953')"
    } else if (media == "ext"){
      mediaCondition = "media_appsid not in ('80000001','80000002','80000006','80000064','80000066','80001011','80001098','80001292','80001539','80002480','80004786','80004787','80004948','80002819','80004944','80004953')"
    }

    val spark = SparkSession.builder()
      .appName(s"calibration_sample")
      .enableHiveSupport()
      .getOrCreate()
    val sql =
      s"""
         |select a.searchid, cast(raw_cvr/1000000 as double) as raw_cvr, substring(adclass,1,6) as adclass,
         |adslot_id, a.ideaid,exp_cvr,unitid,userid,click_unit_count,conversion_from, hour,a.day,
         |if(c.iscvr is not null,1,0) iscvr,case when siteid = 0 then 'wailian' when siteid>=5000000 then 'chitu' when siteid>=2000000 then 'jingyu' else 'laojianzhan' end siteid,
         |case
         |  when user_show_ad_num = 0 then '0'
         |  when user_show_ad_num = 1 then '1'
         |  when user_show_ad_num = 2 then '2'
         |  when user_show_ad_num in (3,4) then '4'
         |  when user_show_ad_num in (5,6,7) then '7'
         |  else '8' end as user_show_ad_num,
         |a.conversion_goal
         |from
         |  (select * from
         |  dl_cpc.cvr_calibration_sample_all
         |  where $selectCondition1
         |  and $mediaCondition
         |  and cvr_model_name in ('$model','$calimodel')
         |  and is_ocpc = 1) a
         | left join
         | (select distinct searchid,conversion_goal,1 as iscvr,conversion_from
         |  from dl_cpc.ocpc_quick_cv_log
         |  where  $selectCondition2) c
         |  on a.searchid = c.searchid and a.conversion_goal = c.conversion_goal and a.conversion_from = c.conversion_from
             """.stripMargin
    println(sql)
    val data = spark.sql(sql)
    val defaultideaid = data.groupBy("ideaid").count()
      .withColumn("ideaidtag",when(col("count")>=10,1).otherwise(0))
      .filter("ideaidtag=1")
    val defaultunitid = data.groupBy("unitid").count()
      .withColumn("unitidtag",when(col("count")>=10,1).otherwise(0))
      .filter("unitidtag=1")
    val defaultuserid = data.groupBy("userid").count()
      .withColumn("useridtag",when(col("count")>=10,1).otherwise(0))
      .filter("useridtag=1")

    val result = data
      .join(defaultideaid,Seq("ideaid"),"left")
      .join(defaultunitid,Seq("unitid"),"left")
      .join(defaultuserid,Seq("userid"),"left")
      .withColumn("ideaidnew",when(col("ideaidtag")===1,col("ideaid")).otherwise(9999999))
      .withColumn("unitidnew",when(col("unitidtag")===1,col("unitid")).otherwise(9999999))
      .withColumn("useridnew",when(col("useridtag")===1,col("userid")).otherwise(9999999))
      .filter("iscvr is not null")
      .select("searchid","ideaid","adclass","adslot_id","iscvr","unitid","raw_cvr","user_show_ad_num",
        "exp_cvr","day","userid","conversion_from","hour","siteid","ideaidnew","unitidnew","useridnew","conversion_goal")
      result.show(10)
    val avgs = result.rdd.map(f => {
      f.mkString("\001")
    })
      .collect()


    printToFile(new File(s"/home/cpc/scheduled_job/hourly_calibration/calibration_sample_${model}.csv"),
      "searchid\001ideaid\001adclass\001adslot_id\001iscvr\001unitid\001raw_cvr\001user_show_ad_num\001exp_cvr\001day\001userid\001conversion_from\001hour\001siteid\001ideaidnew\001unitidnew\001useridnew\001conversion_goal") {
      p => avgs.foreach(p.println) // avgs.foreach(p.println)
    }

   val move =s"hdfs dfs -put -f /home/cpc/scheduled_job/hourly_calibration/calibration_sample_${model}.csv hdfs://emr-cluster/user/cpc/wy/calibration/calibration_sample_${model}_${endDate}-${endHour}-${hourRange}.csv"
    move !
  }

  def printToFile(f: java.io.File,ColumnName:String)(op: java.io.PrintWriter => Unit)
  {
    val p = new java.io.PrintWriter(f);
    p.write(s"$ColumnName\n")
    try { op(p) }
    finally { p.close() }
  }
}
