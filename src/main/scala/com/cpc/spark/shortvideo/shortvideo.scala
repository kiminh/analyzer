package com.cpc.spark.shortvideo
import java.io.FileOutputStream

import com.google.protobuf.struct.Struct
import shortvideo._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.types.{StructField, StructType}
import java.text.SimpleDateFormat
import java.util.Calendar
import shortvideothreshold.shortvideothreshold.ShortVideoThreshold
import shortvideothreshold.shortvideothreshold.ThresholdShortVideo
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import com.cpc.spark.ocpcV3.ocpc.OcpcUtils._
import java.time
import java.io.PrintWriter
import org.apache.spark.sql.functions
import shortvideothreshold.Shortvideothreshold

/*2019-03-21 基础表slim_union_log 换成dl_cpc.cpc_basedata_union_events
 *2019-03-25 增加详情页的media_appsid 80000002
* */
import scala.collection.mutable.ListBuffer

object shortvideo {
  def main(args: Array[String]): Unit = {
    val date = args(0)
    val hour =args (1)
    val traffic = args(2)
    val spark = SparkSession.builder()
      .appName(s"""shortvideo_execute +'${date}'+'${hour}' """)
      .enableHiveSupport()
      .getOrCreate()
    import org.apache.spark.sql._
    import spark.implicits._
    import org.apache.spark.sql._
    import scala.collection.mutable.ListBuffer

    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -168)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql21(date1, hour1, date, hour)
    val selectCondition2 = getTimeRangeSql22(date1, hour1, date, hour)
    val selectCondition3 = getTimeRangeSql23(date1, hour1, date, hour)

    spark.sql("set hive.exec.dynamic.partition=true")
    //  生成中间表 video_mid
     spark.sql(
      s"""
         |insert overwrite table dl_cpc.cpc_union_events_video_mid  partition (dt,hr)
select   searchid, adtype,userid,ideaid,isclick,isreport,exp_cvr_ori,
         exp_cvr,cvr_rank,src,
          label_type,planid,unitid, adclass,view1.adslot_type,label2,view1.uid,
          usertype,view1.adslotid,isshow,price,'${date}' as dt,'${hour}' as hr
from
(
  select     day,hour,a.searchid, isshow,exp_cvr/1000000 as exp_cvr_ori,exp_cvr,isclick,price,cvr_model_name,uid,userid, adslotid,
             charge_type,adtype,ideaid,usertype,adslot_type,adclass, planid,unitid,
             row_number() over (partition by userid  order by exp_cvr desc ) cvr_rank
  from       dl_cpc.cpc_basedata_union_events a
  where    ${selectCondition}
  and      media_appsid in  ("80000001","80000002")
  and      interaction=2
  and     adtype in (2,8,10)
  and     userid>0
  and     usertype in (0,1,2)
  and     isclick=1
  and     adslot_type = 1
  and     adsrc = 1
  and     isshow = 1
  and     ideaid > 0
  and      (charge_type is null or charge_type=1)
  and     uid not like "%.%"
  and     uid not like "%000000%"
  and     length(uid) in (14, 15, 36)
  and     a.searchid not in
        (
         | select searchid
         | from
         |   (
         |   select searchid,exptags,newcol,day
         |   from   dl_cpc.cpc_basedata_union_events
         |   lateral view explode(exptags) tag as newcol
         |   )  c
         |  where  ${selectCondition}
         |  and    newcol='use_strategy'
         |)
) view1
left JOIN
(
  select   `date`,hour hour2,aa.searchid as searchid2,isreport,src,
label_type,  label2
  FROM
  (
    select          `date`,hour,
                     final.searchid as searchid,src,label_type,uid,planid,unitid, adclass,adslot_type,label2,
                     final.ideaid as ideaid,
                     case
          when final.src="elds" and final.label_type=6 then 1
          when final.src="feedapp" and final.label_type in (4, 5) then 1
          when final.src="yysc" and final.label_type=12 then 1
          when final.src="wzcp" and final.label_type in (1, 2, 3) then 1
          when final.src="others" and final.label_type=6 then 1
          else 0     end as isreport
          from
          (
          select  distinct
              `date`,hour,searchid, media_appsid, uid,
              planid, unitid, ideaid, adclass,adslot_type,label2,
              case
                  when (adclass like '134%' or adclass like '107%') then "elds"
                  when (adslot_type<>7 and adclass like '100%') then "feedapp"
                  when (adslot_type=7 and adclass like '100%') then "yysc"
                  when adclass in (110110100, 125100100) then "wzcp"
                  else "others"
              end as src,
              label_type
          from
              dl_cpc.ml_cvr_feature_v1
          where
              ${selectCondition2}
              and label2=1
             and media_appsid in ("80000001","80000002")
            ) final
       ) aa
  where   aa.isreport=1
) a
on  a.searchid2=view1.searchid
and   a.`date`=view1.day
and   a.hour2 =view1.hour
group by searchid, adtype,userid,ideaid,isclick,isreport,exp_cvr_ori,
         exp_cvr,cvr_rank,src,
          label_type,planid,unitid, adclass,view1.adslot_type,label2,view1.uid,
          usertype, adslotid,isshow,price,'${date}' ,'${hour}'
         |""".stripMargin)
    val tab0 = spark.read.table("dl_cpc.cpc_union_events_video_mid").filter(s"dt='${date}' and hr='${hour}'").
      selectExpr(
      "searchid", "adtype ","userid","ideaid","isclick","isreport","expcvr_d",
      "exp_cvr","cvr_rank","src","label_type","planid","unitid","adclass","adslot_type","label2","uid",
      "usertype","adslot_id","isshow","price","dt","hr")
//     tab0.repartition(100).write.mode("overwrite").insertInto("dl_cpc.cpc_union_events_video_mid ")
     println("dl_cpc.cpc_union_events_video_mid insert success!")
      //  动态取threshold,计算每个短视频userid下面所有的exp_cvr，进行排序
     //   RDD方法,获得短视频userid阈值


    val tabb = tab0.rdd.map(row => (row.getAs[String]("userid") ->List(row.getAs[Int]("exp_cvr")))).
      reduceByKey((x, y) => x ::: y).
      mapValues(x => {
        val sorted = x.sorted
        val th0 = 0
        val th1 = sorted((sorted.length * 0.05).toInt)
        val th2 = sorted((sorted.length * 0.10).toInt)
        val th3 = sorted((sorted.length * 0.15).toInt)
        val th4 = sorted((sorted.length * 0.2).toInt)
        val th5 = sorted((sorted.length * 0.25).toInt)
        val th6 = sorted((sorted.length * 0.3).toInt)
        (th0, th1, th2, th3, th4, th5, th6)
      }).map(x=> {
      tabrank(
        userid_d = x._1,
        expcvr_0per = x._2._1,
        expcvr_5per = x._2._2,
        expcvr_10per = x._2._3,
        expcvr_15per = x._2._4,
        expcvr_20per = x._2._5,
        expcvr_25per = x._2._6,
        expcvr_30per = x._2._7
      )
    }).toDS()

//      .toDF("userid_d", "expcvr_0per", "expcvr_5per", "expcvr_10per", "expcvr_15per", "expcvr_20per", "expcvr_25per", "expcvr_30per")
    println("spark 7 threshold success!")
    val tabd=tabb.selectExpr("userid_d", "expcvr_0per", "expcvr_5per", "expcvr_10per", "expcvr_15per", "expcvr_20per", "expcvr_25per", "expcvr_30per",
      s"""'${date}' as dt""",s"""'${hour}' as hr""")
    tabd.write.mode("overwrite").insertInto("dl_cpc.userid_expcvr_lastpercent")
    tabd.show(10,false)

//    val tabc = spark.createDataFrame(tabb)
//    val tabd = tabc.rdd.map(r => {
//      val userid = r.getAs[String](0)
//      userid
//      val rank0per = r.getAs[Int](1)(0).toInt
//       val rank5per = r.getAs[Int](1)(1)
//      val rank10per = r.getAs[Int](1)(2)
//      val rank15per = r.getAs[Int](1)(3)
//      val rank20per = r.getAs[Array[Int]](1)(4)
//      val rank25per = r.getAs[Array[Int]](1)(5)
//      val rank30per = r.getAs[Array[Int]](1)(6)
//      (userid,rank0per, rank5per, rank10per, rank15per, rank20per, rank25per, rank30per)
//    }).map(s => (s._1, s._2, s._3, s._4, s._5, s._6, s._7,s._8)).
//      toDF("userid_d", "expcvr_0per", "expcvr_5per", "expcvr_10per", "expcvr_15per", "expcvr_20per", "expcvr_25per", "expcvr_30per")
//    tabd.show(false)
    println("spark 7 threshold tab success!")

    //计算大图和短视频实际cvr
    val  cvrcomparetab = spark.sql(
      s"""
           |select
           |    userid,case when adtype in (8,10) then 'video' when adtype =2 then 'bigpic' end adtype_cate,adclass,
           |    sum(isshow) as show_num,
           |    sum(isclick) as click_num,
           |    round(sum(isclick)/sum(isshow),6) as ctr,
           |    round(sum(case WHEN isclick = 1 then price else 0 end)*10/sum(isshow), 6) as cpm,
           |    sum(case when isreport=1 then 1  else 0 end ) cvr_n,
           |    round(sum(if(isreport=1,1,0))/sum(isclick),6) as cvr,
           |    round(sum(exp_cvr)/sum(isshow),6) as exp_cvr,
           |    dt,hr
           |from   dl_cpc.cpc_union_events_video_mid
           |where  dt='${date}' and hr='${hour}'
           |group by userid,case when adtype in (8,10) then 'video' when adtype =2 then 'bigpic' end ,adclass,dt,hr
       """.stripMargin).selectExpr("userid","adtype_cate","adclass","show_num","click_num",
    "ctr","cpm","cvr_n","cvr","exp_cvr","dt","hr")
    cvrcomparetab.show(10,false)
    cvrcomparetab.repartition(100).write.mode("overwrite").
      insertInto("dl_cpc.cpc_bigpicvideo_cvr")
      println("compare video bigpic act cvr midtab  success")

    /*######增加该userid没有大图，用所在行业实际cvr来衡量的条件##################################*/
    val  cvrcomparetab2 = spark.sql(
       s"""
          |select   m.adclass,
          |    sum(isshow) as show_num,
          |    sum(isclick) as click_num,
          |    round(sum(isclick)/sum(isshow),6) as ctr,
          |    round(sum(case WHEN isclick = 1 then price else 0 end)*10/sum(isshow), 6) as cpm,
          |    sum(if(isreport=1,1,0)) as convert_num,
          |    sum(case when isreport=1 then 1  else 0  end ) cvr_n,
          |    round(sum(if(isreport=1,1,0))/sum(isclick),6) as act_cvr
          |from
          |(
          |select  substr(adclass,1,6) adclass,isshow,isclick,price,searchid
          |from     dl_cpc.cpc_basedata_union_events
          |where  ${selectCondition}
          |and   adsrc=1
          |and   isshow=1
          |and   isclick=1
          |and   media_appsid in  ("80000001","80000002")
          |and   interaction=2
          |and  adtype=2
          |)   m
          |left join
          |(
          |  select   `date`,hour hour2,aa.searchid as searchid2,isreport,src,substr(adclass,1,6) adclass,
          |             label_type,  label2
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
          |              ${selectCondition2}
          |              and label2=1
          |             and media_appsid in ("80000001","80000002")
          |            ) final
          |       ) aa
          |  where   aa.isreport=1
          |) a
          |on  a.adclass =m.adclass
          |and  a.searchid2 =m.searchid
          |group by m.adclass
         """.stripMargin).selectExpr("adclass","show_num","click_num","ctr","cpm","convert_num","cvr_n","act_cvr",
                s"""'${date}' as dt""",s"""'${hour}' as hr""")
    cvrcomparetab2.show(10,false)
    cvrcomparetab2.write.mode("overwrite").insertInto("dl_cpc.bigpic_adclass_actcvr_mid")

//  合并视频 vs 大图实际cvr vs 行业实际cvr
    val bigpiccvr=spark.sql(
      s"""
         |select video.userid,
         |       video.video_act_cvr1,
         |       bigpic.bigpic_act_cvr,
         |       bigpic.bigpic_expcvr,
         |       adclass.adclass_act_cvr,
         |       video.dt,video.hr
         |from
         |(
         |select userid,adtype_cate,cvr video_act_cvr1,substr(adclass,1,6) adclass,dt,hr
         |from  dl_cpc.cpc_bigpicvideo_cvr
         |where  dt='${date}' and hr='${hour}'
         |and   adtype_cate='video'
         |)   video
         |left join
         |(
         |  select  userid,adtype_cate,cvr  bigpic_act_cvr,exp_cvr bigpic_expcvr
         |  from  dl_cpc.cpc_bigpicvideo_cvr
         |  where  dt='${date}' and hr='${hour}'
         |  and   adtype_cate='bigpic'
         |) bigpic
         |on  bigpic.userid=video.userid
         |left join
         |(
         |  select  adclass ,act_cvr adclass_act_cvr
         |  from dl_cpc.bigpic_adclass_actcvr_mid
         |  where  dt='${date}' and hr='${hour}'
         |
         |) adclass
         |on  adclass.adclass=video.adclass
         |where  ( video_act_cvr1<bigpic_act_cvr or (bigpic_act_cvr is null and video_act_cvr1<adclass_act_cvr ))
      """.stripMargin).selectExpr("userid as userid_b","bigpic_act_cvr","video_act_cvr1","adclass_act_cvr")
    bigpiccvr.show(10,false)
    println(" video_act_cvr1<bigpic_act_cvr  or video_act_cvr1<adclass_act_cvr  userid tab success!")

    //过滤大图cvr<短视频或行业actcvr的userid,待计算剩下的userid 的cvr
    val tab1=tab0.join(bigpiccvr,tab0("userid")===bigpiccvr("userid_b"),"inner").
      selectExpr("userid","isshow","isclick","price","isreport","exp_cvr","video_act_cvr1",
        "bigpic_act_cvr","adclass_act_cvr",s"""'${date}' as dt""",s"""'${hour}' as hr""")
    tab1.write.mode("overwrite").insertInto("dl_cpc.bigpic_adclass_ls_actcvr_userid")
    tab1.show(10,false)
    println(" join tab0 success!")
    //计算短视频cvr
    val taba = spark.sql(
      s"""
         |select
         |     userid,expcvr_0per, expcvr_5per, expcvr_10per, expcvr_15per, expcvr_20per, expcvr_25per, expcvr_30per,
         |     round(sum(if(isreport =1 and exp_cvr>=expcvr_0per and  ${traffic}>=0,1,0))/sum(if(exp_cvr>=expcvr_0per and isclick=1,1,0)),6) as traffic_0per_expcvr,
         |     round(sum(if(isreport =1 and exp_cvr>=expcvr_5per and ${traffic}>=0.05,1,0))/sum(if(exp_cvr>=expcvr_5per and isclick=1,1,0)),6) as traffic_5per_expcvr,
         |     round(sum(if(isreport =1 and exp_cvr>=expcvr_10per and ${traffic}>=0.10,1,0))/sum(if(exp_cvr>=expcvr_10per and isclick=1,1,0)),6) as traffic_10per_expcvr,
         |     round(sum(if(isreport =1 and exp_cvr>=expcvr_15per and ${traffic}>=0.15,1,0))/sum(if(exp_cvr>=expcvr_15per and isclick=1,1,0)),6) as traffic_15per_expcvr,
         |     round(sum(if(isreport =1 and exp_cvr>=expcvr_20per and ${traffic}>=0.20,1,0))/sum(if(exp_cvr>=expcvr_20per and isclick=1,1,0)),6) as traffic_20per_expcvr,
         |     round(sum(if(isreport =1 and exp_cvr>=expcvr_25per and ${traffic}>=0.25,1,0))/sum(if(exp_cvr>=expcvr_25per and isclick=1,1,0)),6) as traffic_25per_expcvr,
         |     round(sum(if(isreport =1 and exp_cvr>=expcvr_30per and ${traffic}>=0.30,1,0))/sum(if(exp_cvr>=expcvr_30per and isclick=1,1,0)),6) as traffic_30per_expcvr,
         |           video_act_cvr1 as video_act_cvr,
         |           bigpic_act_cvr,adclass_act_cvr,
         |           dt,hr
         | from
         | (   select userid,exp_cvr,isshow,isclick,isreport,price,exp_cvr,
         |            bigpic_act_cvr,adclass_act_cvr,video_act_cvr1,dt,hr
         |    from   dl_cpc.bigpic_adclass_ls_actcvr_userid
         |    where  dt='${date}' and hr='${hour}'
         | ) view
         | join
         | (
         |     select  userid userid_d, expcvr_0per, expcvr_5per, expcvr_10per, expcvr_15per, expcvr_20per, expcvr_25per, expcvr_30per
         |     from    dl_cpc.userid_expcvr_lastpercent
         |     where  dt='${date}' and hr='${hour}'
         | )   threshold
         |on    view.userid=threshold.userid_d
         |group by userid,expcvr_0per, expcvr_5per, expcvr_10per, expcvr_15per, expcvr_20per, expcvr_25per, expcvr_30per,
         |      video_act_cvr1,bigpic_act_cvr,adclass_act_cvr,dt,hr
         |""".stripMargin).
      selectExpr("userid","expcvr_0per","expcvr_5per","expcvr_10per", "expcvr_15per", "expcvr_20per", "expcvr_25per", "expcvr_30per",
        "traffic_0per_expcvr","traffic_5per_expcvr","traffic_10per_expcvr","traffic_15per_expcvr","traffic_20per_expcvr",
        "traffic_25per_expcvr","traffic_30per_expcvr","video_act_cvr","bigpic_act_cvr","adclass_act_cvr"
        ,"dt","hr").distinct()
      taba.show(10,false)
      taba.write.mode("overwrite").insertInto("dl_cpc.video_trafficcut_threshold_mid")
      println("dl_cpc.video_trafficcut_threshold_mid  insert success!")
     val sqlfinal=
       s"""
          |select   maxexpcvr.userid, case
          |         when (traffic_0per_expcvr>=bigpic_act_cvr) or ( bigpic_act_cvr is null and  traffic_0per_expcvr>=adclass_act_cvr ) then expcvr_threshold0per
          |         when (traffic_5per_expcvr>=bigpic_act_cvr) or ( bigpic_act_cvr is null and  traffic_5per_expcvr>=adclass_act_cvr ) then expcvr_threshold5per
          |         when (traffic_10per_expcvr>=bigpic_act_cvr) or ( bigpic_act_cvr is null and  traffic_10per_expcvr>=adclass_act_cvr ) then expcvr_threshold10per
          |         when (traffic_15per_expcvr>=bigpic_act_cvr) or ( bigpic_act_cvr is null and  traffic_15per_expcvr>=adclass_act_cvr ) then expcvr_threshold15per
          |         when (traffic_20per_expcvr>=bigpic_act_cvr) or ( bigpic_act_cvr is null and  traffic_20per_expcvr>=adclass_act_cvr ) then expcvr_threshold20per
          |         when (traffic_25per_expcvr>=bigpic_act_cvr) or ( bigpic_act_cvr is null and  traffic_25per_expcvr>=adclass_act_cvr ) then expcvr_threshold25per
          |         when (traffic_30per_expcvr>=bigpic_act_cvr) or ( bigpic_act_cvr is null and  traffic_30per_expcvr>=adclass_act_cvr ) then expcvr_threshold30per
          |         else max_expcvr end  max_expcvr,
          |          dt, hr
          | from
          | (
          |  select    userid,
          |            case when    (expcvr_threshold30per>=expcvr_threshold5per
          |                     and  expcvr_threshold30per>=expcvr_threshold10per
          |                     and  expcvr_threshold30per>=expcvr_threshold15per
          |                     and  expcvr_threshold30per>=expcvr_threshold20per
          |                     and  expcvr_threshold30per>=expcvr_threshold25per
          |                     and  expcvr_threshold30per>=expcvr_threshold0per)
          |                     and  ${traffic}>=0.3         then expcvr_threshold30per
          |                 when    (expcvr_threshold25per>=expcvr_threshold20per
          |				              and  expcvr_threshold25per>=expcvr_threshold15per
          |                     and  expcvr_threshold25per>=expcvr_threshold10per
          |                     and  expcvr_threshold25per>=expcvr_threshold5per
          |                     and  expcvr_threshold25per>=expcvr_threshold0per)
          |                     and   ${traffic}>=0.25      then expcvr_threshold25per
          |                 when    (expcvr_threshold20per>=expcvr_threshold15per
          |				              and  expcvr_threshold20per>=expcvr_threshold10per
          |                     and  expcvr_threshold20per>=expcvr_threshold5per
          |                     and  expcvr_threshold20per>=expcvr_threshold0per)
          |                     and  ${traffic}>=0.2        then expcvr_threshold20per
          |                 when    (expcvr_threshold15per>=expcvr_threshold10per
          |				              and  expcvr_threshold15per>=expcvr_threshold5per
          |                     and  expcvr_threshold15per>=expcvr_threshold0per)
          |                     and  ${traffic}>=0.15       then expcvr_threshold15per
          |                 when    (expcvr_threshold10per>=expcvr_threshold5per
          |				              and  expcvr_threshold10per>=expcvr_threshold0per)
          |                     and  ${traffic}>=0.1        then expcvr_threshold10per
          |                 when    expcvr_threshold5per>=expcvr_threshold0per
          |                     and  ${traffic}>=0.05       then expcvr_threshold5per
          |				          when   ${traffic}>=0.0          then expcvr_threshold0per
          |                 else  expcvr_threshold0per
          |                 end as max_expcvr
          |from   dl_cpc.video_trafficcut_threshold_mid
          |where  dt='${date}' and hr='${hour}'
          | )  maxexpcvr
          | join
          | (
          |   select   *
          |   from    dl_cpc.video_trafficcut_threshold_mid
          |   where   dt='${date}' and hr='${hour}'
          | )  threshold_mid
          |on  maxexpcvr.userid=threshold_mid.userid
          |
        """.stripMargin
     val tabfinal=spark.sql(sqlfinal).selectExpr("userid","max_expcvr as expcvr","dt","hr")
     tabfinal.show(10,false)
    tabfinal.write.mode("overwrite").insertInto("dl_cpc.cpc_appdown_cvr_threshold_mid")

     println("dl_cpc.cpc_appdown_cvr_threshold stage1 insert success!")

     val tabfinal2= spark.sql(
       s"""
          |select userid,expcvr
          |from
          |(
          |select  userid,expcvr,row_number() over (partition by userid order by dt )  ranking
          |from
          |(
          |select  userid ,expcvr,dt
          |from    dl_cpc.cpc_appdown_cvr_threshold_mid
          |where   dt='${date}'  and hr='${hour}'
          |union  all
          |select  userid ,expcvr,dt
          |from    dl_cpc.cpc_appdown_cvr_threshold
          |where   dt=date_add('${date}',-1) and  hr='${hour}'
          |) view2
          |) view3
          |where  ranking=1
          |group by userid,expcvr

        """.stripMargin).
       selectExpr("userid","expcvr",s"""'${date}' as dt""",s"""'${hour}' as dt""")
     tabfinal2.show(10,false)
      tabfinal2.write.mode("overwrite").insertInto("dl_cpc.cpc_appdown_cvr_threshold")
      val tabfinal3=tabfinal2.selectExpr("userid","expcvr","dt")
    /*#########################################################################*/
    //   pb写法2
   //不执行pb文件
    val list = new scala.collection.mutable.ListBuffer[ShortVideoThreshold]()
    var cnt = 0
    for (record <- tabfinal3.collect()) {
      var userid = record.getAs[String]("userid")
      var exp_cvr = record.getAs[Long]("expcvr")
      println(s"""useridr:$userid, expcvr:${exp_cvr}""")

      cnt += 1
      val Item = ShortVideoThreshold(
        userid = userid,
        threshold = exp_cvr
      )
      list += Item
    }
    println("final userid cnt:" + cnt)
    val result = list.toArray
    val ecvr_tslist = ThresholdShortVideo(
      svt = result)
    println("Array length:" + result.length)
    ecvr_tslist.writeTo(new FileOutputStream("shortvideo.pb"))
    println("shortvideo.pb insert success!")



    /*#################################################################################*/

  }

//  def tranTimeToLong(tm:String) :Long= {
//      val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//      val dt = fm.parse(tm)
//      val aa = fm.format(dt)
//      val tim: Long = dt.getTime()
//      tim
//    }

  def getTimeRangeSql21(startDate: String, startHour: String, endDate: String, endHour: String): String = {
    if (startDate.equals(endDate)) {
      return s"( day = '$startDate' and hour <= '$endHour' and hour > '$startHour')"
    }
    return s"((day = '$startDate' and hour >='$startHour') " +
      s"or (day = '$endDate' and hour <'$endHour') " +
      s"or (day > '$startDate' and day < '$endDate'))"
  }

   def getTimeRangeSql22(startDate: String, startHour: String, endDate: String, endHour: String): String = {
  if (startDate.equals(endDate)) {
    return s"(`date` = '$startDate' and hour <= '$endHour' and hour > '$startHour')"
  }
  return s"((`date` = '$startDate' and hour >= '$startHour') " +
    s"or (`date` = '$endDate' and hour < '$endHour') " +
    s"or (`date` > '$startDate' and `date` < '$endDate'))"
}
  def getTimeRangeSql23(startDate: String, startHour: String, endDate: String, endHour: String): String = {
    if (startDate.equals(endDate)) {
      return s"(`date` = '$startDate' and hour <= '$endHour' and hour > '$startHour')"
    }
    return s"((dt = '$startDate' and hr >= '$startHour') " +
      s"or (dt = '$endDate' and hr < '$endHour') " +
      s"or (dt > '$startDate' and dt < '$endDate'))"
  }
  case class tabrank (val  userid_d: String =" ",
                      val  expcvr_0per: Int=0,
                      val  expcvr_5per: Int=0,
                      val  expcvr_10per: Int=0,
                      val  expcvr_15per: Int=0,
                      val  expcvr_20per: Int=0,
                      val  expcvr_25per: Int=0,
                      val  expcvr_30per: Int=0 )

}

/*
中间表 mid
create table if not exists dl_cpc.cpc_union_events_video_mid
(
    searchid string,
    adtype   string,
    userid   string,
    ideaid   int,
    isclick  int,
    isreport int,
    expcvr_d  double,
    exp_cvr int,
    cvr_rank bigint,
    src      string,
    label_type int,
    planid   int,
    unitid   int,
    adclass  int,
    adslot_type  int,
    label2   int,
    uid      string,
    usertype  int,
    adslot_id string,
    isshow   int,
    price    bigint
)
partitioned by (dt string,hr string)
row format delimited fields terminated by '\t' lines terminated by '\n';

--大图和短视频的实际cvr table
create table if not exists dl_cpc.cpc_bigpicvideo_cvr
(
   userid    string,
   adtype_cate    string,   --区分是2-大图的，还是8，10-短视频的
   adclass   string,
   show_num  bigint,
   click_num bigint,
   ctr       double,
   cpm       double,
   cvr_n     bigint,
   cvr       double,
   exp_cvr   double
)
partitioned by (dt string,hr string)
row format delimited fields terminated by '\t' lines terminated by '\n';

---不同流量切分等级的expcvr阈值和expcvr效果
create table if not exists dl_cpc.video_trafficcut_threshold_mid
(
   userid string,
   expcvr_threshold0per  bigint,
   expcvr_threshold5per  bigint,
   expcvr_threshold10per  bigint,
   expcvr_threshold15per  bigint,
   expcvr_threshold20per  bigint,
   expcvr_threshold25per  bigint,
   expcvr_threshold30per  bigint,
   traffic_0per_expcvr    double,
   traffic_5per_expcvr    double,
   traffic_10per_expcvr    double,
   traffic_15per_expcvr    double,
   traffic_20per_expcvr    double,
   traffic_25per_expcvr    double,
   traffic_30per_expcvr    double,
   video_act_cvr           double,
   bigpic_act_cvr          double,
   adclass_act_cvr         double

)
partitioned by (dt string, hr string)
row format delimited fields terminated by '\t' lines terminated by '\n';

--计算useri的降序分位数expcvr表
create table if not exists dl_cpc.userid_expcvr_lastpercent
(
userid   string,
expcvr_0per  bigint,
expcvr_5per  bigint,
expcvr_10per bigint,
expcvr_15per  bigint,
expcvr_20per  bigint,
expcvr_25per  bigint,
expcvr_30per  bigint
)
partitioned by (dt string,hr string)
row format delimited fields terminated by '\t' lines terminated by '\n';

--计算userid 的视频实际cvr小于大图实际cvr或行业实际cvr
create table if not exists dl_cpc.bigpic_adclass_ls_actcvr_userid
(
userid  string,
isshow  bigint,
isclick bigint,
price   double,
isreport  int,
exp_cvr  bigint,
video_act_cvr1  double,
bigpic_act_cvr   double,
adclass_act_cvr  double

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

