package com.cpc.spark.metrics

import org.apache.spark.sql.SparkSession

/**
  * @author Jinbao
  * @date 2019/2/15 17:57
  */
object ocpcMetrics {
    def main(args: Array[String]): Unit = {
        val date = args(0)
        val spark = SparkSession.builder()
          .appName(s"ocpcMetrics date = $date")
          .enableHiveSupport()
          .getOrCreate()
        import spark.implicits._

        val sql1 =
            s"""
               |select count(*) as total_userid_num,
               |  sum(if(auc>0.65,1,0)) as auc_userid_num,
               |  round(sum(if(auc>0.65,1,0))/count(*),6) as auc_userid_rate,
               |  sum(if(auc>0.65 and pcoc >= 0.6 and pcoc <= 1.8,1,0)) as auc_pcoc_userid_num,
               |  round(sum(if(auc>0.65 and pcoc >= 0.6 and pcoc <= 1.8,1,0))/count(*),6) as auc_pcoc_userid_rate
               |from
               |(
               |  select
               |    userid,
               |    round(sum(auc)/count(*),6) as auc,
               |    round(sum(pcvr*click)/sum(cvrcnt),6) as pcoc
               |  from dl_cpc.ocpc_suggest_cpa_recommend_hourly
               |  where `date`='$date'
               |  and original_conversion = 3
               |  and industry = 'elds'
               |  group by userid
               |) x
             """.stripMargin

        val t1 = spark.sql(sql1)

        t1.createOrReplaceTempView("t1")

        val sqlt1 =
            s"""
               |select '总体' as tag, total_userid_num as userid_num, 1.0 as userid_rate from t1
               |union
               |select 'auc满足>0.65' as tag, auc_userid_num as userid_num, auc_userid_rate as userid_rate from t1
               |union
               |select 'auc满足>0.65且pcoc满足0.6<=pcoc<=1.8' as tag, auc_pcoc_userid_num as userid_num, auc_pcoc_userid_rate as userid_rate from t1
             """.stripMargin

        val r1 = spark.sql(sqlt1)

        r1.show(10)
    }
}
