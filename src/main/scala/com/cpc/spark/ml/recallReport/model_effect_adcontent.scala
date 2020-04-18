package com.cpc.spark.ml.recallReport

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.common.Murmur3Hash
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable


object model_effect_adcontent {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("model_effect_adcontent").enableHiveSupport().getOrCreate()
    val curday = args(0)
    val seed = args(1).toInt
    val dist = args(2)
    val hour_start = args(3)
    val hour_end = args(4)
    val threshold = args(5)
    import spark.implicits._
    val cal = Calendar.getInstance()
    cal.setTime(new SimpleDateFormat("yyyy-MM-dd").parse(s"$curday"))
    cal.add(Calendar.DATE, -1)
    val oneday = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val dist_map = mutable.Map[Int, String]()
    val dist_split = dist.split(";")
    for(i <- dist_split){
      val model_name = i.split(",")(0)
      val start = i.split(",")(1).toInt
      val end = i.split(",")(2).toInt
      for(j <- start until end){
        dist_map.update(j, model_name)
      }
    }
    val sql_test =
      s"""
         |select if(length(tuid)>0, tuid, uid) as uid
         |        ,ctr_model_name
         |    from dl_cpc.cpc_basedata_union_events
         |    where day = '$oneday'
         |    and media_appsid in ('80000001','80000002','80000006','80000064','80000066')
         |    and adslot_type = 2
         |    and isshow=1
         |    and ctr_model_name not like '%noctr%'
         |    and adsrc in (1, 28)
         |    and hour = '$hour_end'
         |""".stripMargin
    val dau_log_test = spark.sql(sql_test).withColumn("hash_model_name",hash(seed, dist_map)($"uid"))
    val acc = dau_log_test.filter("hash_model_name=ctr_model_name").count()*1.0/dau_log_test.count()
    if(acc<threshold.toFloat){
      println("hash wrong:%s", acc.toString)
      System.exit(1)
    }
    val sql =
      s"""
         |select A.*, if(B.conversion_goal is not null and isclick=1, 1, 0) as iscvr
         |from
         |(select searchid, ideaid, conversion_goal, if(length(tuid)>0, tuid, uid) as uid
         |        ,charge_type
         |        ,isshow
         |        ,isclick
         |        ,raw_ctr
         |        ,price
         |        ,dsp_cpm
         |        ,adsrc
         |       ,ctr_model_name, cast(bid_ocpc as double) as cpagiven
         |    from dl_cpc.cpc_basedata_union_events
         |    where day = '$oneday'
         |    and hour >= '$hour_start'
         |    and hour <= '$hour_end'
         |    and media_appsid in ('80000001','80000002','80000006','80000064','80000066')
         |    and adslot_type = 2
         |    and isshow=1
         |    and ctr_model_name not like '%noctr%') A
         |    left join
         |    (SELECT searchid, ideaid, conversion_goal FROM dl_cpc.ocpc_cvr_log_hourly WHERE date = '$oneday'
         |    GROUP BY searchid, ideaid, conversion_goal) B ON A.searchid = B.searchid
         |    AND A.ideaid = B.ideaid
         |    AND A.conversion_goal = B.conversion_goal
         |""".stripMargin
    spark.sql(sql).withColumn("hash_model_name",hash(seed, dist_map)($"uid")).createOrReplaceTempView("union_log")
    spark.sql(
      s"""
         |insert overwrite table dl_cpc.cpc_model_effect partition (model_type="adcontent", day="$oneday")
         |select
         |        hash_model_name, uv, imp_all, click_all, rev_all, (click_all/imp_all) as ctr_all,
         |        (rev_all/uv) as arpu_all,(rev_all/imp_all) as cpm_all, (rev_all/click_all) as acp_all,
         |        imp_cpc,click_cpc,rev_cpc,(click_cpc/imp_cpc) as ctr_cpc,(rev_cpc/uv) as arpu_cpc,
         |        (rev_cpc/imp_cpc) as cpm_cpc, (rev_cpc/click_cpc) as acp_cpc,(imp_all/uv) imp_uid,
         |        exp_ctr * imp_cpc/click_cpc as pcoc, cv, cv/imp_all as show_cvr, cv/click_all as cvr,
         |        cv * cpagiven as cv_cpagiven
         |    from (
         |        select
         |            hash_model_name,
         |            count(distinct uid) as uv,
         |            sum(isshow) as imp_all,
         |            sum(isclick) as click_all,
         |            sum(if(isclick > 0 and adsrc in (1, 28), price, 0)
         |            + if(isshow > 0 and adsrc not in (1, 28), dsp_cpm / 1000, 0)) as rev_all,
         |            sum(if(isshow > 0 and adsrc in (1, 28), 1, 0)) as imp_cpc,
         |            sum(if(isclick > 0 and adsrc in (1, 28), 1, 0)) as click_cpc,
         |            sum(if(isclick > 0 and adsrc in (1, 28), price, 0)) as rev_cpc,
         |            sum(if(adsrc in (1, 28),raw_ctr/1000000,0)) / sum(if(adsrc in (1, 28),1,0)) as exp_ctr,
         |            sum(iscvr) as cv,
         |            sum(case when iscvr=1 then cpagiven else 0 end) * 0.01 / sum(iscvr) as cpagiven
         |        from union_log
         |        group by hash_model_name
         |    ) t
         |""".stripMargin)
  }

  def hash(seed:Int, dist_map:mutable.Map[Int, String])= udf {
    x:String => {
      var hash_value = Murmur3Hash.stringHash32(x,seed).toLong
      if(hash_value<0){
        hash_value += 4294967296L
      }
      val dis = hash_value%1000
      dist_map(dis.toInt)
    }
  }

}
