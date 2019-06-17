package com.cpc.spark.OcpcProtoType.model_wz

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcUpdateBudget {
  def main(args: Array[String]): Unit = {
    /*
    pb文件格式：
    string identifier = 1;
    int32 conversiongoal = 2;
    double kvalue = 3;
    double cpagiven = 4;
    int64 cvrcnt = 5;
    对于明投广告，cpagiven=1， cvrcnt使用ocpc广告记录进行关联，k需要进行计算，每个conversiongoal都需要进行计算


     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    // bash: 2019-01-02 12 1 wz qtt
    val date = args(0).toString
    val hour = args(1).toString

    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, 1)
    val tomorrow = calendar.getTime
    val date1 = dateConverter.format(tomorrow)

//    userid                  bigint
//      planid                  bigint
//      unitid                  bigint
//      exp_tag                 string
//      budget                  bigint
//      consume                 bigint
//      is_open                 int
//      date                    string

    val sqlRequest =
      s"""
         |SELECT
         |  userid,
         |  planid,
         |  unitid,
         |  exp_tag,
         |  0 as budget,
         |  0 as consume,
         |  1 as is_open
         |FROM
         |  test.check_unitid_consume
         |WHERE
         |  userid not in (1565736, 1626187)
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    val result = data
      .withColumn("date", lit(date1))

    result
      .repartition(5)
//      .write.mode("overwrite").saveAsTable("test.check_unitid_consume20190612")
      .write.mode("overwrite").insertInto("dl_cpc.check_unitid_consume")


  }


}


