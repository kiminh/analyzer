package com.cpc.spark.ml.calibration.debug

import java.util.Base64

import com.google.protobuf.CodedInputStream
import mlmodel.mlmodel.FeatureStore
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._

/**
  * @author WangYao
  * @date 2019/03/11
  */
object DaliyCheck {
    def main(args: Array[String]): Unit = {
      val date = args(0)
      val hour = args(1)
      val spark = SparkSession.builder()
          .appName(s"Snapshot Analysis date = $date and hour = $hour")
          .enableHiveSupport()
          .getOrCreate()
      import spark.implicits._

      val clicksql = s"""
                        |select a.searchid, cast(a.raw_cvr as bigint) as ectr, substring(a.adclass,1,6) as adclass,
                        |a.cvr_model_name as model, a.adslot_id as adslotid, a.ideaid,b.conversion_target[0] as cvr_target
                        |case
                        |  when a.length(ocpc_log)>0 then 'ocpc'
                        |  when user_cvr_threshold = 200 then "cvr2"
                        |  when user_cvr_threshold >0 then "cvr1"
                        |  else "other"
                        |  end as exp_cvr_type,
                        |case
                        |  when user_req_ad_num = 0 then '0'
                        |  when user_req_ad_num = 1 then '1'
                        |  when user_req_ad_num = 2 then '2'
                        |  when user_req_ad_num in (3,4) then '4'
                        |  when user_req_ad_num in (5,6,7) then '7'
                        |  else '8' end as user_req_ad_num
                        |  from
                        |  (select *
                        |  from dl_cpc.cpc_basedata_union_events
                        |  where day = '$date'
                        |  and media_appsid in ('80000001','80000002') and isclick = 1
                        |  and cvr_model_name in ('qtt-cvr-dnn-rawid-v1-180-newcali','qtt-cvr-dnn-rawid-v1-180')
                        |  and adsrc = 1
                        |  AND (charge_type IS NULL OR charge_type = 1)
                        |  )a
                        |  left join dl_cpc.dw_unitid_detail b
                        |    on a.unitid = b.unitid
                        |    and b.day = '$date'
                        |    and b.conversion_target[0] not in ('none','site_uncertain')
       """.stripMargin
      println(s"sql:\n$clicksql")
      val clickData = spark.sql(clicksql)
      val cvrsql =s"""
                     |select distinct a.searchid,
                     |    a.conversion_target as unit_target,
                     |     b.conversion_target[0] as real_target,
                     |     1 as iscvr
                     |from dl_cpc.cpc_conversions a
                     |join dl_cpc.dw_unitid_detail b
                     |    on a.unitid=b.unitid
                     |    and b.day = '$date'
                     |where a.day = '$date'
                     |  and array_contains(a.conversion_target,b.conversion_target[0])
                     |  and size(a.conversion_target)>0)
       """.stripMargin

      val cvrData = spark.sql(cvrsql)
      val cvrResult = clickData.filter("cvr_target is not null")
        .join(cvrData,col("searchid"),"left")
        .groupBy("model")
    }
}
