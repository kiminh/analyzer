package com.cpc.spark.novel

import org.apache.spark.sql.SparkSession
import com.cpc.spark.tools.OperateMySQL._

object UnitDeliveryType {
  def main(args: Array[String]): Unit = {
    val date = args(0)
    val hour = args(1)
    val spark = SparkSession.builder()
      .appName(s"NovelUnitDeliveryType date = $date and hour = $hour")
      .enableHiveSupport()
      .getOrCreate()
    val sql =
      s"""
         |select
         |  ideaid, delivery_type, unitid, userid, adclass, usertype, adslot_id, day, cast(hour as int) hour,
         |  sum(isshow) imp, sum(isclick) click,
         |  sum(if(isclick = 1,price,0)) cost
         |from dl_cpc.cpc_novel_union_events
         |where day= '$date' and hour = '$hour'
         |  and isshow = 1 and adsrc = 1
         |  AND (charge_type IS NULL OR charge_type = 1)
         |group by ideaid, delivery_type, unitid, userid, adclass, usertype, adslot_id, day, hour
             """.stripMargin
    println(sql)

    val data = spark.sql(sql)
    val reportTable = "report2.report_novel_unit_delivery_type"
    val delSQL = s"delete from $reportTable where `date` = '$date' and hour = $hour"

    update(delSQL) //先删除历史数据
    insert(data, reportTable) //插入数据
  }
}
