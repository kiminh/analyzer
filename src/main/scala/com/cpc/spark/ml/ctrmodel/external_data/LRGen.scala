package com.cpc.spark.ml.ctrmodel.external_data

import com.cpc.spark.ml.dnn.Utils.DateUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * @author fym
  * @version created: 2019-05-28 10:53
  * @desc
  */
object LRGen {

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.WARN)

    val spark = SparkSession.builder().appName("LREval").enableHiveSupport().getOrCreate()

    val date = args(0)
    val tomorrow=DateUtils.getPrevDate(date, -1)

//    val beforeSql=
//      """
//        |create temporary function auc as 'hivemall.evaluation.AUCUDAF' using jar "/home/cpc/anal/lib/hivemall-all-0.5.2-incubating.jar"
//      """.stripMargin
//    spark.sql(beforeSql)

    val bs_sql =
      s"""
         |insert overwrite table dl_cpc.external_data_sample_test_qizhi partition (dt='${date}')
         |select a.*,b.* from
         |(select
         |  searchid
         |  , isclick as label
         |  , sex
         |  , age
         |  , os
         |  , isp
         |  , network
         |  , city
         |  , media_appsid
         |  , phone_level
         |  , `timestamp`
         |  , adtype
         |  , planid
         |  , unitid
         |  , ideaid
         |  , adclass
         |  , adslot_id as adslotid -- bottom-up compatibility.
         |  , adslot_type
         |  , interact_pagenum as pagenum -- bottom-up compatibility.
         |  , interact_bookid as bookid -- bottom-up compatibility.
         |  , brand_title
         |  , user_req_num
         |  , uid
         |  , click_count as user_click_num
         |  , click_unit_count as user_click_unit_num
         |  , long_click_count as user_long_click_count
         |  , phone_price
         |  , province
         |  , city_level
         |  , media_type
         |  , channel
         |  , dtu_id
         |  , interaction
         |  , userid
         |  , is_new_ad
         |  , hour
         |  , md5(uid) as md5uid
         |from dl_cpc.cpc_basedata_union_events
         |where `day`='${date}'
         |  and media_appsid in ('80000001','80000002')
         |  and adslot_type in (1, 2)
         |  and isshow = 1
         |  and ideaid > 0
         |  and unitid > 0)a
         |inner join
         |(select clientid from dl_cpc.external_data_test_qizhi)b
         |on a.md5uid=b.clientid
      """.stripMargin
    println("sql="+bs_sql)
    val bs_df=spark.sql(bs_sql)
    println("============= bs_df ===============")

  }
}