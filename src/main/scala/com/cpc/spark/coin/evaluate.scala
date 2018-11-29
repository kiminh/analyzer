package com.cpc.spark.coin

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.util.Properties
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * @author Jinbao
  * @date 2018/11/26 16:10
  */
object evaluate {
    def main(args: Array[String]): Unit = {
        val date = args(0)
        val spark = SparkSession.builder()
          .appName(s"evaluate date = $date")
          .enableHiveSupport()
          .getOrCreate()

        import spark.implicits._

        val sql =
            s"""
               |select a.*,b.label2 as label2
               |from
               |(
               |    select *
               |    from dl_cpc.cpc_union_log
               |    where `date`='$date'
               |    and media_appsid  in ("80000001", "80000002") and isshow = 1
               |) a left outer join
               |(
               |    select searchid, label2
               |    from dl_cpc.ml_cvr_feature_v1
               |    where `date`='$date'
               |) b
               |on a.searchid = b.searchid
             """.stripMargin
        println(sql)
        val union = spark.sql(sql)
        val testTable = "test.cpc_union_log_label2"
        union.write.mode("overwrite").insertInto(testTable)


    }
}
