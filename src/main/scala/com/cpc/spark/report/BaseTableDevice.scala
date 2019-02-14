package com.cpc.spark.report

import org.apache.spark.sql.{SaveMode, SparkSession}

/** 设备基础数据表
  * 统计这6类的数量
  *   1. uid=""
  *   2. uid.contains(".")
  *   3. uid.contains("000000")
  *   4. uid.length in (15, 16, 17)  imei
  *   5. uid.length = 36  idfa
  *   6. 其他
  */
object BaseTableDevice {
  def main(args: Array[String]): Unit = {
    val day = args(0)

    val spark = SparkSession.builder()
      .appName("base table device")
      .enableHiveSupport()
      .getOrCreate()

    val regex = "^[0-9]*$".r //imei

    val sql =
      s"""
         |select   media_appsid
         |        ,adslot_type
         |        ,adslot_id
         |        ,uid
         |from dl_cpc.cpc_basedata_union_events
         |where day='$day'
         |group by media_appsid
         |        ,adslot_type
         |        ,adslot_id
         |        ,uid
       """.stripMargin
    println("sql: " + sql)

    import spark.implicits._
    val result = spark.sql(sql).repartition(1000)
      .select("media_appsid", "adslot_type", "adslot_id", "uid")
      .rdd
      .map { r =>
        val media_appsid = r.getAs[String]("media_appsid")
        val adslot_type = r.getAs[Int]("adslot_type")
        val adslot_id = r.getAs[String]("adslot_id")
        val uid_type = r.getAs[String]("uid") match {
          case u if u == "" => "empty"
          case u if u.contains(".") => "ip"
          case u if u.contains("000000") => "zero_device"
          case u if (u.length == 15 || u.length == 16 || u.length == 17) && (regex.findFirstMatchIn(u) != None) => "imei"
          case u if (u.length == 36) => "idfa"
          case _ => "other"
        }
        ((media_appsid, adslot_type, adslot_id, uid_type), 1)
      }
      .reduceByKey(_ + _)
      .map { x => ((x._1._1, x._1._2, x._1._3), Map(x._1._4 -> x._2)) }
      .reduceByKey(_ ++ _)
      .map { x =>
        var empty_num = 0
        var ip_num = 0
        var zero_device_num = 0
        var imei_num = 0
        var idfa_num = 0
        var other_num = 0
        x._2.foreach { m =>
          m._1 match {
            case "empty" => empty_num = m._2
            case "ip" => ip_num = m._2
            case "zero_device" => zero_device_num = m._2
            case "imei" => imei_num = m._2
            case "idfa" => idfa_num = m._2
            case "other" => other_num = m._2
            case _ =>
          }
        }
        (x._1._1, x._1._2, x._1._3, empty_num, ip_num, zero_device_num, imei_num, idfa_num, other_num)
      }
      .toDF("media_appsid", "adslot_type", "adslot_id", "empty_num", "ip_num", "zero_device_num", "imei_num", "idfa_num", "other_num")

    println("total: " + result.count())

    result
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"hdfs://emr-cluster2/warehouse/dl_cpc.db/cpc_basedata_device/day=$day")

    spark.sql(
      s"""
         |ALTER TABLE dl_cpc.cpc_basedata_device add if not exists PARTITION(`day` = "$day")
         | LOCATION  'hdfs://emr-cluster2/warehouse/dl_cpc.db/cpc_basedata_device/day=$day'
      """.stripMargin)

    println("base table device")

  }

}
