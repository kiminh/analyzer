package com.cpc.spark.coin

import org.apache.spark.sql.SparkSession

/**
  * @author Jinbao
  * @date 2018/11/28 11:29
  */
object EvaluationAutoCoin {
    def main(args: Array[String]): Unit = {
        val date = args(0)
        val spark = SparkSession.builder()
          .appName(s"EvaluationAutoCoin date = $date")
          .enableHiveSupport()
          .getOrCreate()

        import spark.implicits._

        val sql =
            s"""
               |select *
               |from
               |(
               |    select *
               |    from dl_cpc.cpc_union_log
               |    where `date`='$date'
               |    and media_appsid  in ("80000001", "80000002") and isshow = 1
               |    and ext['antispam'].int_value = 0 and ideaid > 0
               |    and adsrc = 1
               |    and ext['city_level'].int_value != 1
               |    and userid not in (1001028, 1501875)
               |) a left outer join
               |(
               |    select searchid, label2
               |    from dl_cpc.ml_cvr_feature_v1
               |    where `date`='$date'
               |) b
               |on a.searchid = b.searchid
             """.stripMargin

        val union = spark.sql(sql)
        val testTable = "test.union_feature"
        union.write.mode("overwrite").saveAsTable(testTable)

        val ideaidSql =
            s"""
               |select
               |    '$date' as `date`
               |    ideaid,
               |    sum(isclick) as click_num,
               |    sum(case WHEN isclick = 1 and ext_int['exp_style'] =510127 then 1 else 0 end) as coin_click_num,
               |    round(sum(case WHEN isclick = 1 and ext_int['exp_style'] =510127 then 1 else 0 end)/sum(case WHEN isshow = 1 and ext_int['exp_style'] =510127 then 1 else 0 end),6) as coin_ctr,
               |    round(sum(case WHEN label2 = 1 and ext_int['exp_style'] =510127 then 1 else 0 end)/sum(case WHEN isclick = 1 and ext_int['exp_style'] =510127 then 1 else 0 end),6) as coin_cvr,
               |    sum(if (isclick = 1 , price, 0)) click_price,
               |    sum(if (isclick = 1 and ext_int['exp_style'] =510127 , price, 0)) auto_coin_price
               |from $testTable
               |group by ideaid
             """.stripMargin

        val ideaidEvaluation = spark.sql(ideaidSql)

        ideaidEvaluation.show(5)

        ideaidEvaluation.write.mode("overwrite").insertInto("dl_cpc.auto_coin_evaluation_ideaid_daily")
        println("insert into dl_cpc.auto_coin_evaluation_ideaid_daily success!")


    }
}
