package com.cpc.spark.coin

import org.apache.spark.sql.SparkSession

/**
  * @author Jinbao
  * @date 2018/12/5 0:11
  */
object CoinTest {
    def main(args: Array[String]): Unit = {
        val date = args(0)

        val spark = SparkSession.builder()
          .appName(s"Coin date = $date")
          .enableHiveSupport()
          .getOrCreate()

        val sql =
            s"""
               |select *
               |from dl_cpc.cpc_union_log
               |where `date`='$date'
               |and isshow = 1
               |and ext_int['exp_style'] = 510127
             """.stripMargin

        val all = spark.sql(sql)

        all.write.mode("overwrite").insertInto("test.test_coin_1204")
    }
}
