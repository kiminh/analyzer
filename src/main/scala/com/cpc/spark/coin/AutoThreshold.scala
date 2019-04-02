package com.cpc.spark.coin

import org.apache.spark.sql.SparkSession

/**
  * @author Jinbao
  * @date 2019/2/25 17:38
  */
object AutoThreshold {
    def main(args: Array[String]): Unit = {
        val date = args(0)
        val spark = SparkSession.builder()
          .appName(s"AutoThreshold date = $date")
          .enableHiveSupport()
          .getOrCreate()
        import spark.implicits._

        val min_p = args(1).toDouble            //0.6
        val max_p = args(2).toDouble            //1.0
        val default_p = args(3).toDouble        //0.8
        val negative_alpha = args(4).toDouble    //0.02
        val positive_alpha = args(5).toDouble   //0.04

        val sql =
            s"""
               |select ideaid,
               |    min_p,
               |    max_p,
               |    case when p+alpha < min_p then min_p
               |         when p+alpha > max_p then max_p
               |         else p+alpha end
               |         as p,
               |    alpha,
               |    flag,
               |    '$date' as `date`
               |from
               |(
               |  select
               |    if (b.ideaid is null, a.ideaid, b.ideaid) as ideaid,
               |    $min_p as min_p,
               |    $max_p as max_p,
               |    if(a.p is null, $default_p, a.p) as p,
               |    case when b.flag = 0 then $negative_alpha
               |         when b.flag = 1 then 0-$positive_alpha
               |         else 0 end as alpha,
               |    b.flag as flag
               |  from
               |  (
               |    select
               |        ideaid, min_p, max_p, p, alpha, flag
               |    from dl_cpc.cpc_auto_coin_idea_threshold
               |    where `date` = date_sub('$date', 1)
               |  ) a
               |  full outer join
               |  (
               |    select ideaid,if(coin_cvr<cvr, 0, 1) as flag
               |    from dl_cpc.cpc_report_coin_ideaid_metrics
               |    where `date` = date_sub('$date', 1)
               |    and coin_click_num > 10
               |  ) b
               |  on a.ideaid = b.ideaid
               |) final
             """.stripMargin

        println(sql)

        val result = spark.sql(sql).repartition(1)

        result.write.mode("overwrite").insertInto("dl_cpc.cpc_auto_coin_idea_threshold")

        println("success!")
    }
}

/*

create table if not exists dl_cpc.cpc_auto_coin_idea_threshold
(
    ideaid int,
    min_p  double,
    max_p  double,
    p      double,
    alpha  double,
    flag   double
)
PARTITIONED BY (`date` string)
STORED AS PARQUET;
 */