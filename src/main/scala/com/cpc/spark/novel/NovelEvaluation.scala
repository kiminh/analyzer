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
               |    sum(case when isshow = 1 and adsrc = 1 then 1 else 0 end) as cpc_show_num, --cpc展示数
               |    sum(isshow) as show_num, --总展示数
               |    round(sum(case when isshow = 1 and adsrc = 1 then 1 else 0 end)/ sum(isshow), 4) as cpc_show_rate,
               |    sum(case when isclick = 1 and adsrc = 1 then 1 else 0 end) as cpc_click_num, --cpc点击数
               |    sum(isclick) as click_num, --点击数
               |    round(sum(isclick) / sum(isshow),6) as ctr, --点击率
               |    round(sum(case when isclick = 1 and adsrc = 1 then 1 else 0 end) / sum(case when isshow = 1 and adsrc = 1 then 1 else 0 end), 6) as cpc_ctr, --cpc点击率
               |    sum(case WHEN isclick = 1 then price else 0 end) / 100.0 as total_price, --点击总价
               |    sum(case WHEN isclick = 1 and adsrc = 1 then price else 0 end) / 100.0 as cpc_total_price, --cpc点击总价
               |    round(sum(case WHEN isclick = 1 and adsrc = 1 then price else 0 end)*10/sum(case when isshow = 1 and adsrc = 1 then 1 else 0 end), 6) as cpc_cpm,
               |    round(sum(case WHEN isclick = 1 and adsrc = 1 then price else 0 end)*10/count(distinct case when adsrc = 1 then uid else null end), 6) as cpc_arpu,
               |    '$date' as date
               |from dl_cpc.cpc_novel_union_log
               |where `date` = '$date'
             """.stripMargin

        val novelEval = spark.sql(sql)

        novelEval.repartition(1)
          .write
          .mode("overwrite")
          .insertInto("dl_cpc.report_novel_evaluation")

        println("insert into dl_cpc.report_novel_evaluation success!")

        val novelEvalDetailSql =
            s"""
               |select
               |    case when adslotid in ("7515276", "7765361") then "插入页"
               |        when adslotid in ("7479769", "7199174") then "章节尾"
               |        when adslotid in ("7251427") then "互动"
               |        else "其他" end as tag,
               |    sum(case when isshow = 1 and adsrc = 1 then 1 else 0 end) as cpc_show_num, --cpc展示数
               |    sum(isshow) as show_num, --总展示数
               |    round(sum(case when isshow = 1 and adsrc = 1 then 1 else 0 end)/ sum(isshow), 4) as cpc_show_rate,
               |    sum(case when isclick = 1 and adsrc = 1 then 1 else 0 end) as cpc_click_num, --cpc点击数
               |    sum(isclick) as click_num, --点击数
               |    round(sum(isclick) / sum(isshow),6) as ctr, --点击率
               |    round(sum(case when isclick = 1 and adsrc = 1 then 1 else 0 end) / sum(case when isshow = 1 and adsrc = 1 then 1 else 0 end), 6) as cpc_ctr, --cpc点击率
               |    sum(case WHEN isclick = 1 then price else 0 end) / 100.0 as total_price, --点击总价
               |    sum(case WHEN isclick = 1 and adsrc = 1 then price else 0 end) / 100.0 as cpc_total_price, --cpc点击总价
               |    round(sum(case WHEN isclick = 1 and adsrc = 1 then price else 0 end)*10/sum(case when isshow = 1 and adsrc = 1 then 1 else 0 end), 6) as cpc_cpm,
               |    round(sum(case WHEN isclick = 1 and adsrc = 1 then price else 0 end)*10/count(distinct case when adsrc = 1 then uid else null end), 6) as cpc_arpu,
               |    '$date' as `date`
               |from dl_cpc.cpc_novel_union_log
               |where `date` = '$date'
               |group by
               |case when adslotid in ("7515276", "7765361") then "插入页"
               |    when adslotid in ("7479769", "7199174") then "章节尾"
               |    when adslotid in ("7251427") then "互动"
               |    else "其他" end
             """.stripMargin

        val novelEvalDetail = spark.sql(novelEvalDetailSql)

        novelEvalDetail.repartition(1)
          .write
          .mode("overwrite")
          .insertInto("dl_cpc.report_novel_evaluation_detail")

        println("insert into dl_cpc.report_novel_evaluation_detail success!")
    }
}
