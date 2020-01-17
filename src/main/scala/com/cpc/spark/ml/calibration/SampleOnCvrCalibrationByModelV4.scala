package com.cpc.spark.ml.calibration

import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.cpc.spark.ml.checktool.OutputScoreDifference.hash64
import com.cpc.spark.ocpc.OcpcUtils.{getTimeRangeSql, getTimeRangeSql4}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.sys.process._

object SampleOnCvrCalibrationByModelV4 {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val endDate = args(0)
    val endHour = args(1)
    val hourRange = args(2).toInt
    val model = args(3)
    val calimodel = args(4)
    val media = args(5)
    val task = args(6)
    val sample_date = args(7)

    val endTime = LocalDateTime.parse(s"$endDate-$endHour", DateTimeFormatter.ofPattern("yyyy-MM-dd-HH"))
    val startTime = endTime.minusHours(Math.max(hourRange - 1, 0))
    val startDate = startTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val startHour = startTime.format(DateTimeFormatter.ofPattern("HH"))

    println(s"endDate=$endDate")
    println(s"endHour=$endHour")
    println(s"hourRange=$hourRange")
    println(s"startDate=$startDate")
    println(s"startHour=$startHour")
    println(s"sample_date=$sample_date")
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
         |select a.searchid, if(cvr_model_name in ('$model','$calimodel')
         |,cast(raw_cvr/1000000 as double),0) as raw_cvr,
         |substring(adclass,1,6) as adclass,
         |adslot_id, a.ideaid,exp_cvr,unitid,userid,click_unit_count,a.conversion_from, hour,a.day,
         |if(c.iscvr is not null,1,0) iscvr,case when siteid = 0 then 'wailian' when siteid>=5000000 then 'chitu' when siteid>=2000000 then 'jingyu' else 'laojianzhan' end siteid,
         |case
         |  when user_show_ad_num = 0 then '0'
         |  when user_show_ad_num = 1 then '1'
         |  when user_show_ad_num = 2 then '2'
         |  when user_show_ad_num in (3,4) then '4'
         |  when user_show_ad_num in (5,6,7) then '7'
         |  else '8' end as user_show_ad_num,
         |  a.cvr_model_name,
         |a.conversion_goal
         |from
         |  (select * from
         |  dl_cpc.cvr_calibration_sample_all
         |  where $selectCondition1
         |  and $mediaCondition
         |  and is_ocpc = 1) a
         | left join
         | (select distinct searchid,conversion_goal,1 as iscvr,conversion_from
         |  from dl_cpc.ocpc_quick_cv_log
         |  where  $selectCondition2) c
         |  on a.searchid = c.searchid and a.conversion_goal = c.conversion_goal and a.conversion_from = c.conversion_from
             """.stripMargin
    println(sql)

    val data = spark.sql(sql)
    data.show(10)

    val dnn_data = spark.read.parquet(s"hdfs://emr-cluster/user/cpc/wy/dnn_model_score_offline/calibration/$task/$sample_date/result-*")
      .toDF("id","prediction","num")

    dnn_data.show(10)

    println("dnn model sample is %d".format(dnn_data.count()))
    // get union log

    val union_sample = data
//      .union(data.filter(s"cvr_model_name in ('$model','$calimodel','qtt-cvr-dnn-rawid_novel_jisu_tuid_v2')"))
      .withColumn("id",hash64(0)(col("searchid")))
      .join(dnn_data,Seq("id"),"left")
      .withColumn("raw_cvr",when(col("prediction").isNull,col("raw_cvr")).otherwise(col("prediction")))
      .filter("raw_cvr > 0")

    val wrong_data = union_sample
      .groupBy("unitid")
      .agg(count(col("iscvr")).alias("click"),
        sum(col("iscvr")).alias("iscvr"))
      .withColumn("cvr",col("iscvr")/col("click"))
      .filter("click > 100")
      .filter("cvr > 0.8")
      .withColumn("flag",lit(1))

    println("######  filter unitid  ######")
    wrong_data.show(10)

    val result = union_sample.join(wrong_data.select("unitid","flag"),Seq("unitid"),"left")
      .withColumn("flag",when(col("flag").isNull,lit(0)).otherwise(col("flag")))
      .filter("flag = 0")
      .select("searchid","ideaid","adclass","adslot_id","iscvr","unitid","raw_cvr","user_show_ad_num",
        "exp_cvr","day","userid","conversion_from","hour","siteid","conversion_goal")

      result.show(10)

    println("sample num: %d",result.count())
    val avgs = result.rdd.map(f => {
      f.mkString("\001")
    })
      .coalesce(1).saveAsTextFile("hdfs://emr-cluster/user/cpc/wy/calibration/calibration_sample_${model}_${endDate}-${endHour}-${hourRange}/")


//    printToFile(new File(s"/home/cpc/scheduled_job/hourly_calibration/calibration_sample_${model}.csv"),
//      "searchid\001ideaid\001adclass\001adslot_id\001iscvr\001unitid\001raw_cvr\001user_show_ad_num\001exp_cvr\001day\001userid\001conversion_from\001hour\001siteid\001conversion_goal") {
//      p => avgs.foreach(p.println) // avgs.foreach(p.println)
//    }
//
//   val move =s"hdfs dfs -put -f /home/cpc/scheduled_job/hourly_calibration/calibration_sample_${model}.csv hdfs://emr-cluster/user/cpc/wy/calibration/calibration_sample_${model}_${endDate}-${endHour}-${hourRange}.csv"
//    move !
  }

  def printToFile(f: java.io.File,ColumnName:String)(op: java.io.PrintWriter => Unit)
  {
    val p = new java.io.PrintWriter(f);
    p.write(s"$ColumnName\n")
    try { op(p) }
    finally { p.close() }
  }
}
