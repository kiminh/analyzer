package com.cpc.spark.novel

import org.apache.spark.sql.SparkSession

/**
  * @author Jinbao
  * @date 2018/12/8 14:36
  */
object NovelEvaluation {
    def main(args: Array[String]): Unit = {
        val date = args(0)
        val spark = SparkSession.builder()
          .appName(s"NovelEvaluation date = $date")
          .enableHiveSupport()
          .getOrCreate()
        val sql =
            s"""
               |select
               |    sum(isshow) as show_num, --展示数
               |    sum(isclick) as click_num, --点击数
               |    round(sum(isclick) / sum(isshow),6) as ctr, --点击率
               |    sum(case WHEN isclick = 1 then price else 0 end) as click_total_price, --点击总价
               |    round(sum(case WHEN isclick = 1 then price else 0 end)*10/sum(isshow), 6) as cpm,
               |    '$date' as date
               |from dl_cpc.cpc_novel_union_log
               |where `date` = '$date'
               |and adsrc = 1
             """.stripMargin

        val novelEval = spark.sql(sql)

        novelEval.repartition(1)
          .write
          .mode("overwrite")
          .insertInto("dl_cpc.report_novel_evaluation")

    }
}
