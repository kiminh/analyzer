package com.cpc.spark.ocpc

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, concat_ws, sum, when}

object OcpcHistoryData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // calculate time period for historical data
    val end_date = args(0)
    val hour = args(1)
    //    val threshold = args(2).toInt  //default: 20
    val threshold = 20
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val date = sdf.parse(end_date)
    val calendar = Calendar.getInstance
    calendar.setTime(date)
    calendar.add(Calendar.DATE, -7)
    val dt = calendar.getTime
    val start_date = sdf.format(dt)
    val selectCondition1 = s"`date`='$start_date' and hour > '$hour'"
    val selectCondition2 = s"`date`>'$start_date' and `date`<'$end_date'"
    val selectCondition3 = s"`date`='$end_date' and hour <= '$hour'"

    // read data and set redis configuration
    val sqlRequest =
      s"""
         |SELECT
         |  userid,
         |  uid,
         |  adclass,
         |  SUM(cost) as cost,
         |  SUM(ctr_cnt) as ctr_cnt,
         |  SUM(cvr_cnt) as cvr_cnt,
         |  SUM(total_cnt) as total_cnt
         |FROM
         |  dl_cpc.ocpc_uid_userid_track
         |WHERE ($selectCondition1) OR
         |($selectCondition2) OR
         |($selectCondition3)
         |GROUP BY userid, uid, adclass
       """.stripMargin
    println(sqlRequest)

//    val base = spark.sql(sqlRequest)
//
//    // calculation by userid
//    val userData = base
//      .groupBy(col("userid"), col("adclass"))
//      .agg(sum("cost").alias("cost"), sum("ctr_cnt").alias("user_ctr_cnt"), sum("cvr_cnt").alias("user_cvr_cnt"))
//
//    // calculate by adclass
//    val adclassData = base
//      .groupBy("adclass")
//      .agg(sum("ctr_cnt").alias("adclass_ctr_cnt"), sum("cvr_cnt").alias("adclass_cvr_cnt"))
//
//    // connect adclass and userid
//    val useridAdclassData = userData.join(adclassData, Seq("adclass")).select("userid", "cost", "user_ctr_cnt", "user_cvr_cnt", "adclass_ctr_cnt", "adclass_cvr_cnt")
//
////    val df = useridAdclassData
////      .withColumn("ctr_cnt", when(col("user_cvr_cnt")<20, col("user_ctr_cnt")).otherwise(col("adclass_ctr_cnt")))
////      .withColumn("cvr_cnt", when(col("user_cvr_cnt")<20, col("user_cvr_cnt")).otherwise(col("adclass_cvr_cnt")))
////      .select("userid", "cost", "ctr_cnt", "cvr_cnt")
//
//    useridAdclassData.show()
//
//    useridAdclassData.write.mode("overwrite").saveAsTable("test.historical_ctr_cvr_data")
//
//
//    // step2
//    val sql2 =
//      s"""
//         |SELECT
//         |  a.searchid,
//         |  a.uid,
//         |  a.userid,
//         |  a.ext['exp_ctr'].int_value as exp_ctr,
//         |  a.ext['exp_cvr'].int_value as exp_cvr,
//         |  b.cost,
//         |  b.ctr_cnt as history_ctr_cnt,
//         |  b.cvr_cnt as history_cvr_cnt,
//         |  b.cvr_cnt / b.ctr_cnt as history_cvr
//         |FROM
//         |  (
//         |        select *
//         |        from dl_cpc.cpc_union_log
//         |        where `date`='$end_date' and hour = '$hour'
//         |        and isclick is not null
//         |        and media_appsid  in ("80000001", "80000002")
//         |        and isshow = 1
//         |        and ext['antispam'].int_value = 0
//         |        and ideaid > 0
//         |        and adsrc = 1
//         |        and adslot_type in (1,2,3)
//         |      ) a
//         |INNER JOIN
//         |  (
//         |    SELECT
//         |      userid,
//         |      cost,
//         |      (case when user_cvr_cnt < 20 then adclass_ctr_cnt
//         |        else user_ctr_cnt end) ctr_cnt,
//         |      (case when user_cvr_cnt < 20 then adclass_cvr_cnt
//         |        else user_cvr_cnt end) cvr_cnt
//         |    FROM
//         |      test.historical_ctr_cvr_data) b
//         |ON
//         |  a.userid=b.userid
//       """.stripMargin
//
//    val resultDF = spark.sql(sql2)
//    resultDF.write.mode("overwrite").saveAsTable("test.historical_union_log_data")



  }

}