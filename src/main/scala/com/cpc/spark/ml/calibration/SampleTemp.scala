package com.cpc.spark.ml.calibration

import java.io.File
import sys.process._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat_ws, when}

object SampleTemp {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val date = args(0)
    val spark = SparkSession.builder()
      .appName(s"midu_sample")
      .enableHiveSupport()
      .getOrCreate()
    //穿山甲直投米读
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
         |  else '8' end as user_show_ad_num
         |from
         |  (select * from
         |  dl_cpc.cvr_calibration_sample_all
         |  where day in ('2019-11-27')
         |  and cvr_model_name =  "qtt-cvr-dnn-rawid-v1wzjfgoal"
         |  and is_ocpc = 1) a
         | left join
         | (select distinct searchid,conversion_goal,1 as iscvr
         |  from dl_cpc.ocpc_quick_cv_log
         |  where  `date` in ('2019-11-27')) c
         |  on a.searchid = c.searchid and a.conversion_goal = c.conversion_goal
             """.stripMargin
    println(sql)
    val data = spark.sql(sql)
    val defaultideaid = data.filter("day='2019-11-27'").groupBy("ideaid").count()
      .withColumn("ideaidtag",when(col("count")>=10,1).otherwise(0))
      .filter("ideaidtag=1")
    val defaultunitid = data.filter("day='2019-11-27'").groupBy("unitid").count()
      .withColumn("unitidtag",when(col("count")>=10,1).otherwise(0))
      .filter("unitidtag=1")
    val defaultuserid = data.filter("day='2019-11-27'").groupBy("userid").count()
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
      .filter("day = '2019-11-27'")
      .select("searchid","ideaid","adclass","adslot_id","iscvr","unitid","raw_cvr","user_show_ad_num",
        "exp_cvr","day","userid","conversion_from","hour","siteid","ideaidnew","unitidnew","useridnew")
      result.show(10)
    val avgs = result.rdd.map(f => {
      f.mkString("\001")
    })
      .collect()


    printToFile(new File("/home/cpc/wy/calibration_sample/calibration_sample-v1wzjf-1127.csv"),
      "searchid\001ideaid\001adclass\001adslot_id\001iscvr\001unitid\001raw_cvr\001user_show_ad_num\001exp_cvr\001day\001userid\001conversion_from\001hour\001siteid\001ideaidnew\001unitidnew\001useridnew") {
      p => avgs.foreach(p.println) // avgs.foreach(p.println)
    }

   val move ="hdfs dfs -put -f /home/cpc/wy/calibration_sample/calibration_sample-v1wzjf-1127.csv hdfs://emr-cluster/user/cpc/wy/calibration_sample-v1wzjf-1127.csv"
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
