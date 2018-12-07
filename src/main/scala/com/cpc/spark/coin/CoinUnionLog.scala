package com.cpc.spark.coin

import org.apache.spark.sql.SparkSession

/**
  * @author Jinbao
  * @date 2018/12/7 15:13
  */
object CoinUnionLog {
    def main(args: Array[String]): Unit = {
        val date = args(0)
        val hour = args(1)
        val spark = SparkSession.builder()
          .appName(s"CoinUnionLog date = $date, hour = $hour")
          .enableHiveSupport()
          .getOrCreate()

        val sql =
            s"""
               |select *
               |from dl_cpc.cpc_union_log
               |where `date` = '$date' and hour = '$hour'
               |and ext_int['exp_style'] = 510127
             """.stripMargin

        val coinUnionLog = spark.sql(sql)

        println(coinUnionLog.count())

        coinUnionLog.repartition(200)
          .write
          .mode("overwrite")
          .insertInto("dl_cpc.coin_union_log")
    }
}
