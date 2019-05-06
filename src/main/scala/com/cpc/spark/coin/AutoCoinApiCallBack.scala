package com.cpc.spark.coin

import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * @author Jinbao
  * @date 2019/4/10 16:06
  */
object AutoCoinApiCallBack {
    def main(args: Array[String]): Unit = {
        val day = args(0)
        val hour = args(1)
        val spark = SparkSession.builder()
          .appName(s"AutoCoinApiCallBack day = $day, hour = $hour")
          .enableHiveSupport()
          .getOrCreate()

        import spark.implicits._

        val dayFilter = get3DaysBefore(day, hour)

        val basedataSql =
            s"""
               |select searchid, ideaid, media_appsid, adslot_type, adclass, userid, exp_cvr, usertype
               |from dl_cpc.cpc_basedata_union_events
               |where ($dayFilter)
               |and isclick = 1
               |and is_api_callback = 1
               |and adslot_type != 7
             """.stripMargin
        println(basedataSql)
        val traceSql =
            s"""
               |select searchid
               |from dl_cpc.cpc_basedata_trace_event
               |where day = '$day'
               |and hour = '$hour'
               |and trace_type = 'active_third'
             """.stripMargin
        println(traceSql)
        val result = spark.sql(basedataSql).join(broadcast(spark.sql(traceSql)),Seq("searchid"))
          .withColumn("day",lit(day))
          .withColumn("hour",lit(hour))
          .select("searchid", "ideaid", "media_appsid", "adslot_type",
              "adclass", "userid", "exp_cvr", "usertype", "day", "hour")

//        val sql =
//            s"""
//               |select
//               |    a.searchid as searchid
//               |    ,ideaid
//               |    ,media_appsid
//               |    ,adslot_type
//               |    ,adclass
//               |    ,userid
//               |    ,exp_cvr
//               |    ,usertype
//               |    ,'$day' as day
//               |    ,'$hour' as hour
//               |from
//               |(
//               |    select searchid, ideaid, media_appsid, adslot_type, adclass, userid, exp_cvr, usertype
//               |    from dl_cpc.cpc_basedata_union_events
//               |    where ($dayFilter)
//               |    and isclick = 1
//               |    and is_api_callback = 1
//               |    and adslot_type != 7
//               |) a
//               |join
//               |(
//               |    select searchid
//               |    from dl_cpc.cpc_basedata_trace_event
//               |    where day = '$day'
//               |    and hour = '$hour'
//               |    and trace_type = 'active_third'
//               |) b
//               |on a.searchid = b.searchid
//             """.stripMargin
//
//        println(sql)
//
//        val basedata = spark.sql(sql)

        result.repartition(1)
          .write
          .mode("overwrite")
          .insertInto("dl_cpc.api_basedata_union_events")


    }

    def get3DaysBefore(date: String, hour: String): String = {
        val dateHourList = ListBuffer[String]()
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val cal = Calendar.getInstance()
        cal.set(date.substring(0, 4).toInt, date.substring(5, 7).toInt - 1, date.substring(8, 10).toInt, hour.toInt, 0, 0)
        for (t <- 0 to 72) {
            if (t > 0) {
                cal.add(Calendar.HOUR, -1)
            }
            val formatDate = dateFormat.format(cal.getTime)
            val datee = formatDate.substring(0, 10)
            val hourr = formatDate.substring(11, 13)

            val dateL = s"(`day`='$datee' and `hour`='$hourr')"
            dateHourList += dateL
        }

        "(" + dateHourList.mkString(" or ") + ")"
    }

}
/*
CREATE TABLE IF NOT EXISTS dl_cpc.api_basedata_union_events
(
    searchid string,
    ideaid int,
    media_appsid string,
    adslot_type int,
    adclass int,
    userid int,
    exp_cvr int,
    usertype int
)
PARTITIONED BY (day string, hour string)
STORED AS PARQUET;
 */
