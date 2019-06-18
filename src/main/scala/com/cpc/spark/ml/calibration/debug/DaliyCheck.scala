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

        val sql =
            s"""
               |select a.searchid, cast(a.exp_cvr as bigint) as ecvr,a.isclick,
               |a.cvr_model_name as model,
               |case
               | when length(ocpc_log)>0 then 'ocpc'
               | when user_cvr_threshold = 200 then 'cvr2'
               | when user_cvr_threshold >0 then 'cvr1'
               | else 'other'
               | end as exp_cvr_type,
               |case
               |  when user_req_ad_num = 0 then '0'
               |  when user_req_ad_num = 1 then '1'
               |  when user_req_ad_num = 2 then '2'
               |  when user_req_ad_num in (3,4) then '4'
               |  when user_req_ad_num in (5,6,7) then '7'
               |  else '8' end as user_req_ad_num
               |from dl_cpc.cpc_basedata_union_events a
               |join dl_cpc.dw_unitid_detail b
               |    on a.unitid = b.unitid
               |    and b.day = '{1}'
               |    and b.conversion_target[1] not in ('none','site_uncertain')
               |  where a.day = '{0}'
               |  and a.media_appsid in ('80000001','80000002') and a.isclick = 1
               |  and a.cvr_model_name in ('qtt-cvr-dnn-rawid-v1-180-newcali','qtt-cvr-dnn-rawid-v1-180')
               |  and a.adsrc = 1
               |  AND (charge_type IS NULL OR charge_type = 1)
             """.stripMargin

        println(sql)
        val data = spark.sql(sql)

        data.show(10)
      data.write.mode("overwrite").saveAsTable("test.wy00")
    }
}
