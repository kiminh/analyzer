package com.cpc.spark.shortvideo

import java.io.FileOutputStream
import shortvideothreshold.shortvideothreshold._
import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import java.time
import java.io.PrintWriter

import scala.collection.mutable.ListBuffer

object shortvideo {
  def main(args: Array[String]): Unit = {
    val datetime = args(0)

    val spark = SparkSession.builder()
      .appName(s"shortvideo_execute '${datetime}'")
      .enableHiveSupport()
      .getOrCreate()
    import org.apache.spark.sql._
    import spark.implicits._
    import org.apache.spark.sql._
    import scala.collection.mutable.ListBuffer
    var cala = Calendar.getInstance()
    cala.add(Calendar.HOUR_OF_DAY, -75)
    val date3d = new SimpleDateFormat("yyyy-MM-dd HH:00:00").format(cala.getTime)
    val date3d2 = new SimpleDateFormat("yyyy-MM-dd").format(cala.getTime)
    val date72h = tranTimeToLong(date3d)
    val calb = Calendar.getInstance()
    calb.add(Calendar.HOUR_OF_DAY, -3)
    val datetd = new SimpleDateFormat("yyyy-MM-dd").format(calb.getTime)
    val hourtd = new SimpleDateFormat("HH").format(calb.getTime)


    spark.sql("set hive.exec.dynamic.partition=true")
    //  生成中间表 appdownload_mid
    val sql =
      s"""
         |insert overwrite table dl_cpc.cpc_unionevents_appdownload_mid partition ( dt,hr )
         |select   searchid,`timestamp`,adtype,userid,ideaid,isclick,isreport,exp_cvr_ori,exp_cvr,cvr_rank,src,
         |         label_type,planid,unitid, adclass,adslot_type,label2,uid,usertype,'${datetd}','${hourtd}'
         |from
         |(
         |  select     day,hour,`timestamp`,searchid as searchid,isshow,isclick,usertype,userid,ideaid,adtype,interaction,adsrc,media_appsid,price,exp_cvr exp_cvr_ori,
         |             case when isclick=1 then exp_cvr *1.0 /1000000 end exp_cvr,charge_type,
         |             row_number() over (partition by userid  order by exp_cvr desc ) cvr_rank
         |  from       dl_cpc.cpc_basedata_union_events
         |  where    `timestamp`>='${date72h}'
         |  and      media_appsid in  ("80000001","80000002")
         |  and      interaction=2
         |  and     adtype in (2,8,10)
         |  and     userid>0
         |  and     usertype in (0,1,2)
         |  and     isclick=1
         |) view1
         |left JOIN
         |(
         |  select   `date`,hour hour2,aa.searchid as searchid2,isreport, src,label_type,uid,planid,unitid, adclass,adslot_type,label2
         |  FROM
         |  (
         |    select          `date`,hour,
         |                     final.searchid as searchid,src,label_type,uid,planid,unitid, adclass,adslot_type,label2,
         |                     final.ideaid as ideaid,
         |                     case
         |          when final.src="elds" and final.label_type=6 then 1
         |          when final.src="feedapp" and final.label_type in (4, 5) then 1
         |          when final.src="yysc" and final.label_type=12 then 1
         |          when final.src="wzcp" and final.label_type in (1, 2, 3) then 1
         |          when final.src="others" and final.label_type=6 then 1
         |          else 0     end as isreport
         |          from
         |          (
         |          select  distinct
         |              `date`,hour,searchid, media_appsid, uid,
         |              planid, unitid, ideaid, adclass,adslot_type,label2,
         |              case
         |                  when (adclass like '134%' or adclass like '107%') then "elds"
         |                  when (adslot_type<>7 and adclass like '100%') then "feedapp"
         |                  when (adslot_type=7 and adclass like '100%') then "yysc"
         |                  when adclass in (110110100, 125100100) then "wzcp"
         |                  else "others"
         |              end as src,
         |              label_type
         |          from
         |              dl_cpc.ml_cvr_feature_v1
         |          where
         |              `date`>='${date3d2}'
         |              and label2=1
         |             and media_appsid in ("80000001", "80000002")
         |            ) final
         |       ) aa
         |  where   aa.isreport=1
         |) a
         |on  a.searchid2=view1.searchid
         |and   a.`date`=view1.day
         |and   a.hour2 =view1.hour
         |group by searchid,adtype,userid,ideaid,isclick,isreport,exp_cvr_ori,exp_cvr,cvr_rank,src,label_type,planid,unitid, adclass,adslot_type,label2,uid,usertype
       """.stripMargin
    val tab = spark.sql(sql).cache
    tab.repartition(100).write.mode("overwrite").insertInto("dl_cpc.cp_unionevents_appdownload_qbj")

    //   生成最终表
    val sql2 =
      s"""
         |insert overwrite table dl_cpc.cpc_adddown_cvr_threshold partition (dt,hr)
         | select userid1 userid, exp_cvr expcvr_threshold,'${datetd}','${hourtd}'
         | from
         | (
         | select dt dt1, userid userid1, exp_cvr, cvr_rank, searchid
         | from dl_cpc.cpc_unionevents_appdownload_mid
         | where `timestamp` >= '${date72h}'
         | and adtype in ('8','10')
         | ) rank
         |left join
         |(
         | select dt dt2, userid userid2, max (cvr_rank) as nums
         | from dl_cpc.cpc_unionevents_appdownload_mid
         | where `timestamp`>= '${date72h}'
         | and adtype in ('8','10')
         | group by dt, userid
         |) nums
         | on rank.dt1 = nums.dt2
         | and rank.userid1 = nums.userid2
         | where cvr_rank * 1.0 / nums = 0.9
         | group by userid1, exp_cvr
         | """.stripMargin
    var tab2 = spark.sql(sql2).select("userid", "expcvr_threshold").toDF("userid", "exp_cvr")
    println("result tab count:" + tab2.count())
    tab2.repartition(100).write.mode("overwrite").insertInto("dl_cpc.cpc_appdown_cvr_threshold")
    //    val tab3= tab2.select("userid","expcvr_threshold").toDF("userid","exp_cvr")
    //   pb写法2

    val list = new scala.collection.mutable.ListBuffer[ShortVideoThreshold]()
    var cnt = 0
    for (record <- tab2.collect()) {
      var userid = record.getAs[String]("userid")
      var exp_cvr = record.getAs[Long]("exp_cvr")
      println(s"""useridr:$userid, expcvr:${exp_cvr}""")

      cnt += 1
      val Item = ShortVideoThreshold(
        userid = userid,
        threshold = exp_cvr
      )
      list += Item
    }

    val result = list.toArray
    val ecvr_tslist = ThresholdShortVideo(
      svt = result)


    println("Array length:" + result.length)
    ecvr_tslist.writeTo(new FileOutputStream("shortvideo.pb"))

  }

  def tranTimeToLong(tm:String) :Long= {
      val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val dt = fm.parse(tm)
      val aa = fm.format(dt)
      val tim: Long = dt.getTime()
      tim
    }
//  case class adcvr (var userid : String="",
//                    var exp_cvr : Int=0)



}

/*
中间表 mid
create table if not exists dl_cpc.cpc_unionevents_appdownload_mid
(
    searchid string,
    timestamp     int,
    adtype   string,
    userid   string,
    ideaid   int,
    isclick  int,
    isreport int,
    exp_cvr  int,
    expcvr_d double,
    cvr_rank bigint,
    src      string,
    label_type int,
    planid   int,
    unitid   int,
    adclass  int,
    adslot_type  int,
    label2   int,
    uid      string,
    usertype  int
)
partitioned by (dt string,hr string)
row format delimited fields terminated by '\t' lines terminated by '\n';



pb文件的表结构
create table  if not exists dl_cpc.cpc_appdown_cvr_threshold
(
userid   string comment'广告主id',
expcvr_threshold   bigint comment'expcvr阈值'

)
partitioned by (dt string, hr string)
row format delimited fields terminated by '\t' lines terminated by '\n'
*/