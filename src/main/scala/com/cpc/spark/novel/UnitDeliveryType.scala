package com.cpc.spark.novel

import org.apache.spark.sql.SparkSession
import com.cpc.spark.tools.OperateMySQL

object UnitDeliveryType {
  def main(args: Array[String]): Unit = {
    val date = args(0)
    val hour = args(1)
    val spark = SparkSession.builder()
      .appName(s"NovelUnionLog date = $date and hour = $hour")
      .enableHiveSupport()
      .getOrCreate()
    val sql =
      s"""
         |select ideaid, delivery_type, unitid, userid, sum(isshow) imp,sum(isclick) click,
         |cost, adslot_id, adsrc, adtype, day, hour
         |from dl_cpc.cpc_novel_union_events
         |where day= '$date' and hour = '$hour'
         |and isshow = 1
         |AND (charge_type IS NULL OR charge_type = 1)
             """.stripMargin

    println(sql)

    val reportTableUnit = "report2.report_ocpc_data_detail_v2"
    val delSQLunit = s"delete from $reportTableUnit where `date` = '$date' and hour = $hourInt"

    OperateMySQL.update(delSQLunit) //先删除历史数据
    OperateMySQL.insert(dataUnitMysql, reportTableUnit) //插入数据
  }
}
