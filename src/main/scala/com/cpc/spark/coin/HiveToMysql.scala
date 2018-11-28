package com.cpc.spark.coin

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.util.Properties
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * @author Jinbao
  * @date 2018/11/28 10:13
  */
object HiveToMysql {
    def main(args: Array[String]): Unit = {
        val date = args(0)
        val spark = SparkSession.builder()
          .appName(s"HiveToMysql date = $date")
          .enableHiveSupport()
          .getOrCreate()

        import spark.implicits._

        val sql =
            s"""
               |select *
               |from dl_cpc.auto_coin_evaluation_daily
               |where `date` = '$date'
             """.stripMargin

        println(sql)
        val data = spark.sql(sql)

        data.show(10)

        val conf = ConfigFactory.load()
        val tableName = "report2.report_auto_coin_evaluation_daily"
        val mariadb_write_prop = new Properties()

        val mariadb_write_url = conf.getString("mariadb.report2_write.url")
        mariadb_write_prop.put("user", conf.getString("mariadb.report2_write.user"))
        mariadb_write_prop.put("password", conf.getString("mariadb.report2_write.password"))
        mariadb_write_prop.put("driver", conf.getString("mariadb.report2_write.driver"))

        println("mariadb_write_url = " + mariadb_write_url)

        data.write.mode(SaveMode.Append)
          .jdbc(mariadb_write_url, tableName, mariadb_write_prop)
        println("insert into report2.report_auto_coin_evaluation_daily success!")
    }
}
