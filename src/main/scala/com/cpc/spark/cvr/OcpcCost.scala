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
               |select unitid,price,ocpc_log,label2,media_appsid
               |from(
               |    select searchid,unitid,
               |        case when price > 0 then price else 0 end as price,
               |        ocpc_log,
               |        media_appsid
               |    from dl_cpc.cpc_basedata_union_events
               |    where `date`='$date' and hour = '$hour'
               |    and media_appsid in ('80000001', '80000002', '80001098', '80001292')
               |    and isclick = 1
               |) a
               |left outer join
               |(
               |    select searchid,
               |    case when label2 = 1 then 1 else 0 end as label2
               |    from dl_cpc.ml_cvr_feature_v1
               |    where `date`='$date' and hour = '$hour'
               |    and media_appsid in ('80000001', '80000002', '80001098', '80001292')
               |) b
               |on a.searchid = b.searchid
             """.stripMargin
        println(sql)
        val all = spark.sql(sql)
        //println("all 's count is " + all.count())
        all.createOrReplaceTempView("all")

        val qttSql =
            s"""
               |select unitid,
               |    sum(price) as total_price,
               |    sum(label2) as convert_num,
               |    round(sum(price) / sum(label2),3) as cost,
               |    "qtt" as tag,
               |    '$date' as date,
               |    '$hour' as hour
               |from all
               |where media_appsid in ('80000001', '80000002')
               |group by unitid
             """.stripMargin
        println(qttSql)
        val qtt = spark.sql(qttSql)
        //println("qtt 's count is " + qtt.count())
        //qtt.show(10)

        val miduSql =
            s"""
               |select unitid,
               |    sum(price) as total_price,
               |    sum(label2) as convert_num,
               |    round(sum(price) / sum(label2),3) as cost,
               |    "midu" as tag,
               |    '$date' as date,
               |    '$hour' as hour
               |from all
               |where media_appsid in ('80001098', '80001292')
               |group by unitid
             """.stripMargin
        println(miduSql)

        val midu = spark.sql(miduSql)
        //midu.show(10)
        //println("midu 's count is " + midu.count())
        val midu_ocpcSql =
            s"""
               |select unitid,
               |    sum(price) as total_price,
               |    sum(label2) as convert_num,
               |    round(sum(price) / sum(label2),3) as cost,
               |    "midu_ocpc" as tag,
               |    '$date' as date,
               |    '$hour' as hour
               |from all
               |where media_appsid in ('80001098', '80001292')
               |and length(ocpc_log) > 0
               |group by unitid
             """.stripMargin
        println(midu_ocpcSql)
        val midu_ocpc = spark.sql(midu_ocpcSql)

        //midu_ocpc.show(10)
        //println("midu_ocpc 's count is " + midu_ocpc.count())
        val result = qtt.union(midu).union(midu_ocpc)

        //result.show(10)

        //println("result 's count is " + result.count())
        result.repartition(1)
          .write
          .mode("overwrite")
          .insertInto("dl_cpc.report_cost")

        println("insert into dl_cpc.report_cost success!")
    }

}
