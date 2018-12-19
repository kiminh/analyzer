package com.cpc.spark.novel

import org.apache.spark.sql.SparkSession

/**
  * @author Jinbao
  * @date 2018/12/19 21:25
  */
object NovelCheck {
    def main(args: Array[String]): Unit = {
        val date = args(0)
        val spark = SparkSession.builder()
          .appName(s"NovelCheck date = $date")
          .enableHiveSupport()
          .getOrCreate()
        import spark.implicits._

        val sql =
            s"""
               |select uid,
               |  substring(from_unixtime(timestamp,'yyyy-MM-dd HH:mm:ss'),0,10) as `date`,
               |  substring(from_unixtime(timestamp,'yyyy-MM-dd HH:mm:ss'),12,2) as hour,
               |  substring(from_unixtime(timestamp,'yyyy-MM-dd HH:mm:ss'),15,2) as minute,
               |  sum(isshow) as show_num,
               |  sum(isclick) as click_num,
               |  sum(case WHEN isclick = 1 then price else 0 end) as click_total_price
               |from dl_cpc.cpc_novel_union_log
               |where `date` = '2018-12-19'
               |and adsrc = 1
               |and isshow = 1
               |and uid = '1551055'
               |group by uid,
               |substring(from_unixtime(timestamp,'yyyy-MM-dd HH:mm:ss'),0,10),
               |substring(from_unixtime(timestamp,'yyyy-MM-dd HH:mm:ss'),12,2),
               |substring(from_unixtime(timestamp,'yyyy-MM-dd HH:mm:ss'),15,2)
               |order by uid,
               |substring(from_unixtime(timestamp,'yyyy-MM-dd HH:mm:ss'),0,10),
               |substring(from_unixtime(timestamp,'yyyy-MM-dd HH:mm:ss'),12,2),
               |substring(from_unixtime(timestamp,'yyyy-MM-dd HH:mm:ss'),15,2)
             """.stripMargin

        val d = spark.sql(sql)

        d.repartition(1).write.mode("overwrite")
          .saveAsTable("test.novelcheck")
    }
}
