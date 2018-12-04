package com.cpc.spark.cvr

import org.apache.spark.sql.SparkSession

/**
  * @author Jinbao
  * @date 2018/12/4 15:57
  */
object OcpcCost {
    def main(args: Array[String]): Unit = {
        val date = args(0)
        val hour = args(1)

        val spark = SparkSession.builder()
          .appName(s"OcpcCost date = $date, hour = $hour")
          .enableHiveSupport()
          .getOrCreate()

        val sql =
            s"""
               |select unitid,price,is_ocpc,label2,media_appsid
               |from(
               |    select searchid,unitid,price,ext_int['is_ocpc'] as is_ocpc,media_appsid
               |    from dl_cpc.cpc_union_log
               |    where `date`='$date' and hour = '$hour'
               |    and media_appsid in ('80000001', '80000002', '80001098', '80001292')
               |    and isclick = 1
               |) a
               |left outer join
               |(
               |    select searchid,label2
               |    from dl_cpc.ml_cvr_feature_v1
               |    where `date`='$date' and hour = '$hour'
               |    and media_appsid in ('80000001', '80000002', '80001098', '80001292')
               |) b
               |on a.searchid = b.searchid
             """.stripMargin

        val all = spark.sql(sql)

        all.createOrReplaceTempView("all")

        val qttSql =
            s"""
               |select unitid,
               |    sum(price) as total_price,
               |    sum(case when label2 = 1 then 1 else 0) as convert_num,
               |    "qtt" as tag
               |from all
               |where media_appsid in ('80000001', '80000002')
               |group by unitid
             """.stripMargin

        val qtt = spark.sql(qttSql)

        qtt.show(10)
    }
}
