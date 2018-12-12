package com.cpc.spark.novel

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}

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
               |  a.`date` as `date`,                                      --date
               |  b.show_num as show_num,                                  --米读小说广告总展示数
               |  a.cpc_dsp_show_num as cpc_dsp_show_num,                  --CPC和DSP总展示数
               |  round(a.cpc_dsp_show_num/b.show_num,6)  as cpc_dsp_rate, --CPC和DSP展示占比
               |  a.cpc_show_num as cpc_show_num,                          --CPC展示数
               |  a.cpc_rate as cpc_rate,                                  --CPC展示占比
               |  b.click_num as click_num,                                --米读小说广告总点击数
               |  a.cpc_dsp_click_num as cpc_dsp_click_num,                --CPC和DSP总点击数
               |  a.cpc_dsp_ctr as cpc_dsp_ctr,                            --CTR
               |  a.cpc_click_num as cpc_click_num,                        --CPC点击数
               |  a.cpc_ctr as cpc_ctr,                                    --CPC的CTR
               |  a.cpc_dsp_total_price as cpc_dsp_total_price,            --CPC和DSP总消费
               |  a.cpc_total_price as cpc_total_price,                    --CPC消费
               |  a.cpc_cpm as cpc_cpm,                                    --CPC的CPM
               |  a.cpc_arpu as cpc_arpu                                   --CPC的arpu
               |from
               |(
               |  select
               |    sum(isshow) as cpc_dsp_show_num, --CPC和DSP总展示数
               |    sum(case when isshow = 1 and adsrc = 1 then 1 else 0 end) as cpc_show_num, --cpc展示数
               |    round(sum(case when isshow = 1 and adsrc = 1 then 1 else 0 end)/ sum(isshow), 4) as cpc_rate, --CPC展示占比
               |    sum(isclick) as cpc_dsp_click_num, --CPC和DSP总点击数
               |    round(sum(isclick) / sum(isshow),6) as cpc_dsp_ctr, --点击率
               |    sum(case when isclick = 1 and adsrc = 1 then 1 else 0 end) as cpc_click_num, --cpc点击数
               |    round(sum(case when isclick = 1 and adsrc = 1 then 1 else 0 end) / sum(case when isshow = 1 and adsrc = 1 then 1 else 0 end), 6) as cpc_ctr, --CPC的CTR
               |    sum(case WHEN isclick = 1 then price else 0 end) / 100.0 as cpc_dsp_total_price, --CPC和DSP总消费
               |    sum(case WHEN isclick = 1 and adsrc = 1 then price else 0 end) / 100.0 as cpc_total_price, --CPC消费
               |    round(sum(case WHEN isclick = 1 and adsrc = 1 then price else 0 end)*10/sum(case when isshow = 1 and adsrc = 1 then 1 else 0 end), 6) as cpc_cpm,
               |    round(sum(case WHEN isclick = 1 and adsrc = 1 then price else 0 end)*10/count(distinct case when adsrc = 1 then uid else null end), 6) as cpc_arpu,
               |    '$date' as `date`
               |  from dl_cpc.cpc_novel_union_log
               |  where `date` = '$date'
               |) a join
               |(
               |  select day as `date`,
               |    sum(case when eventid in ('86','88','90','92','98','110','123','125','127','129','131','133','160') then 1 else 0 end ) as show_num,  -- 米读小说广告总展示数
               |    sum(case when eventid in ('87','89','91','93','99','111','124','126','128','130','132','134','159') then 1 else 0 end ) as click_num -- 米读小说广告总点击数
               |  from bdm_book.xcx_web_log_cmd_byday
               |  where day = '$date'
               |  and cmd in ('25101','25003','26002','26003')
               |  group by day
               |) b
               |on a.`date` = b.`date`
             """.stripMargin

        val novelEval = spark.sql(sql).cache()

        novelEval.repartition(1)
          .write
          .mode("overwrite")
          .insertInto("dl_cpc.report_novel_evaluation")

        println("insert into dl_cpc.report_novel_evaluation success!")

        val conf = ConfigFactory.load()
        val mariadb_write_prop = new Properties()

        val mariadb_write_url = conf.getString("mariadb.report2_write.url")
        mariadb_write_prop.put("user", conf.getString("mariadb.report2_write.user"))
        mariadb_write_prop.put("password", conf.getString("mariadb.report2_write.password"))
        mariadb_write_prop.put("driver", conf.getString("mariadb.report2_write.driver"))

        println("mariadb_write_url = " + mariadb_write_url)
        novelEval.write.mode(SaveMode.Append)
          .jdbc(mariadb_write_url, "report2.report_novel_evaluation", mariadb_write_prop)
        println("insert into report2.report_novel_evaluation success!")
        novelEval.unpersist()

//        val novelEvalDetailSql =
//            s"""
//               |select
//               |    case when adslotid in ("7515276", "7765361") then "插入页"
//               |        when adslotid in ("7479769", "7199174") then "章节尾"
//               |        when adslotid in ("7251427") then "互动"
//               |        else "其他" end as tag,
//               |    sum(case when isshow = 1 and adsrc = 1 then 1 else 0 end) as cpc_show_num, --cpc展示数
//               |    sum(isshow) as show_num, --总展示数
//               |    round(sum(case when isshow = 1 and adsrc = 1 then 1 else 0 end)/ sum(isshow), 4) as cpc_show_rate,
//               |    sum(case when isclick = 1 and adsrc = 1 then 1 else 0 end) as cpc_click_num, --cpc点击数
//               |    sum(isclick) as click_num, --点击数
//               |    round(sum(isclick) / sum(isshow),6) as ctr, --点击率
//               |    round(sum(case when isclick = 1 and adsrc = 1 then 1 else 0 end) / sum(case when isshow = 1 and adsrc = 1 then 1 else 0 end), 6) as cpc_ctr, --cpc点击率
//               |    sum(case WHEN isclick = 1 then price else 0 end) / 100.0 as total_price, --点击总价
//               |    sum(case WHEN isclick = 1 and adsrc = 1 then price else 0 end) / 100.0 as cpc_total_price, --cpc点击总价
//               |    round(sum(case WHEN isclick = 1 and adsrc = 1 then price else 0 end)*10/sum(case when isshow = 1 and adsrc = 1 then 1 else 0 end), 6) as cpc_cpm,
//               |    round(sum(case WHEN isclick = 1 and adsrc = 1 then price else 0 end)*10/count(distinct case when adsrc = 1 then uid else null end), 6) as cpc_arpu,
//               |    '$date' as `date`
//               |from dl_cpc.cpc_novel_union_log
//               |where `date` = '$date'
//               |group by
//               |case when adslotid in ("7515276", "7765361") then "插入页"
//               |    when adslotid in ("7479769", "7199174") then "章节尾"
//               |    when adslotid in ("7251427") then "互动"
//               |    else "其他" end
//             """.stripMargin
//
//        val novelEvalDetail = spark.sql(novelEvalDetailSql).cache()
//
//        novelEvalDetail.repartition(1)
//          .write
//          .mode("overwrite")
//          .insertInto("dl_cpc.report_novel_evaluation_detail")
//
//        println("insert into dl_cpc.report_novel_evaluation_detail success!")
//
//        novelEvalDetail.write.mode(SaveMode.Append)
//          .jdbc(mariadb_write_url, "report2.report_novel_evaluation_detail", mariadb_write_prop)
//        println("insert into report2.report_novel_evaluation_detail success!")
//        novelEvalDetail.unpersist()
    }
}
