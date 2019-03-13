package com.cpc.spark.ocpcV3.HP

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._


object videoPromotion {
  def main(args: Array[String]): Unit ={
    val spark = SparkSession.builder().appName("videoPromotion").enableHiveSupport().getOrCreate()

    val date = args(0).toString

    val sql1 =
      s"""
         |  select
         |   case
         |     when exptags like '%use_strategy%' then 'A'
         |     else 'B'
         |    end as test_tag,
         |   t1.searchid,
         |   uid,
         |   adclass,
         |   userid,
         |   case
         |     when adtype = 2 then 'bigImage'
         |     else 'video'
         |    end as adtype1,
         |   ideaid,
         |   isshow,
         |   isclick,
         |   charge_type,
         |   price,
         |   t2.iscvr,
         |   exp_cvr
         |   from (
         |    select *
         |    from dl_cpc.slim_union_log
         |where dt = '$date'
         |  and adtype in (2, 8, 10)
         |  and media_appsid = '80000001'
         |  and adslot_type = 1 --列表页
         |  and adsrc = 1
         |  and userid >0
         |  and isshow = 1
         |  and antispam = 0
         |  and (charge_type is NULL or charge_type = 1)
         |  and interaction=2 --下载
         |  and uid not like "%.%"
         |  and uid not like "%000000%"
         |  and length(uid) in (14, 15, 36)
         |  and ideaid > 0
         |      	) t1
         |   left join (
         |   	   select
         |        searchid,
         |        label2 as iscvr --是否转化
         |       from dl_cpc.ml_cvr_feature_v1
         |      WHERE `date` = '$date'
         |      ) t2
         |   on t1.searchid = t2.searchid
       """.stripMargin

    val baseData = spark.sql(sql1)

    val userAdType = baseData
        .select("userid", "adtype1", "ideaid")
      .distinct()
      .groupBy("userid", "adtype1" )
      .agg(count("ideaid").alias("ad_num"))
      .filter("adtype1 = 'video' and ad_num > 0 ")
        .select("userid")





    userAdType.write.mode("overwrite").saveAsTable("test.user_ad_type_sjq")


  }

}
