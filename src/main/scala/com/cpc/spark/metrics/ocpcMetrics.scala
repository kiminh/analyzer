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

        val unionSql =
            s"""
               |select unitid,userid
               |from dl_cpc.slim_union_log
               |  where dt = '$date'
               |  and media_appsid in ('80000001', '80000002')
               |  and isclick=1
               |  and antispam = 0
               |  and ideaid > 0
               |  and adsrc = 1
               |  and adslot_type in (1,2,3)
               |  and industry = 'feedapp'
               |  and is_api_callback = 1
               |  group by unitid,userid
             """.stripMargin

        val union = spark.sql(unionSql)

        union.createOrReplaceTempView("union")

        //二类电商监控
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
               |select '总体' as tag, total_userid_num as userid_num, 1.0 as userid_rate,'$date' as `date` from t1
               |union
               |select 'auc满足>0.65' as tag, auc_userid_num as userid_num, auc_userid_rate as userid_rate,'$date' as `date` from t1
               |union
               |select 'auc满足>0.65且pcoc满足0.6<=pcoc<=1.8' as tag, auc_pcoc_userid_num as userid_num, auc_pcoc_userid_rate as userid_rate,'$date' as `date` from t1
             """.stripMargin

        val r1 = spark.sql(sqlt1)

        r1.repartition(1).write.mode("overwrite").insertInto("dl_cpc.cpc_ocpc_elds_metrics")

        r1.show(10)
        //app api监控
        val sql2 =
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
               |  and original_conversion = 2
               |  and industry = 'feedapp'
               |  group by userid
               |) x
               |join
               |(
               |  select distinct userid
               |  from union
               |) y
               |on x.userid = y.userid
             """.stripMargin
        val t2 = spark.sql(sql2)

        t2.createOrReplaceTempView("t2")

        val sqlt2 =
            s"""
               |select '总体' as tag, total_userid_num as userid_num, 1.0 as userid_rate,'$date' as `date` from t2
               |union
               |select 'auc满足>0.65' as tag, auc_userid_num as userid_num, auc_userid_rate as userid_rate,'$date' as `date` from t2
               |union
               |select 'auc满足>0.65且pcoc满足0.6<=pcoc<=1.8' as tag, auc_pcoc_userid_num as userid_num, auc_pcoc_userid_rate as userid_rate,'$date' as `date` from t2
             """.stripMargin

        val r2 = spark.sql(sqlt2)

        r2.repartition(1).write.mode("overwrite").insertInto("dl_cpc.cpc_ocpc_app_api_metrics")

        r2.show(10)



        //二类电商监控
        val sql3 =
            s"""
               |select
               |  count(1) as total_unitid_num,
               |  sum(cost) as total_cost,
               |  sum(if(is_recommend=1 and ocpc_flag=0,1,0)) as ocpc_unitid_num,
               |  round(sum(if(is_recommend=1 and ocpc_flag=0,1,0))/count(*),6) as ocpc_unitid_rate,
               |  sum(if(is_recommend=1 and ocpc_flag=0,cost,0)) as ocpc_cost,
               |  round(sum(if(is_recommend=1 and ocpc_flag=0,cost,0))/sum(cost),6) as ocpc_cost_rate,
               |  sum(if(conversion<60,1,0)) as cv_unitid_num,
               |  round(sum(if(conversion<60,1,0))/count(*),6) as cv_unitid_rate,
               |  sum(if(conversion<60,cost,0)) as cv_cost,
               |  round(sum(if(conversion<60,cost,0))/sum(cost),6) as cv_cost_rate,
               |  sum(if(auc<0.65,1,0)) as auc_unitid_num,
               |  round(sum(if(auc<0.65,1,0))/count(*),6) as auc_unitid_rate,
               |  sum(if(auc<0.65,cost,0)) as auc_cost,
               |  round(sum(if(auc<0.65,cost,0))/sum(cost),6) as auc_cost_rate,
               |  sum(if(cal_bid/acb<0.7 or cal_bid/acb>1.3,1,0)) as calc_unitid_num,
               |  round(sum(if(cal_bid/acb<0.7 or cal_bid/acb>1.3,1,0))/count(*),6) as calc_unitid_rate,
               |  sum(if(cal_bid/acb<0.7 or cal_bid/acb>1.3,cost,0)) as calc_cost,
               |  round(sum(if(cal_bid/acb<0.7 or cal_bid/acb>1.3,cost,0))/sum(cost),6) as calc_cost_rate
               |from
               |(
               |  select unitid,
               |    round(sum(cost)/count(*),6) as cost,
               |    round(sum(cvrcnt)/count(*),6) as conversion,
               |    round(sum(auc)/count(*),6) as auc,
               |    round(sum(cal_bid)/count(*),6) as cal_bid,
               |    round(sum(acb)/count(*),6) as acb,
               |    round(sum(is_recommend)/count(*),6) as is_recommend,
               |    round(sum(ocpc_flag)/count(*),6) as ocpc_flag
               |  from dl_cpc.ocpc_suggest_cpa_recommend_hourly
               |  where `date`='$date'
               |  and original_conversion = 3
               |  and industry = 'elds'
               |  group by unitid
               |) x
             """.stripMargin

        val t3 = spark.sql(sql3)
        t3.createOrReplaceTempView("t3")

        val sqlt3 =
            s"""
               |select '二类电商总体' as tag,total_unitid_num as unitid_num,1.0 as unitid_num_rate,total_cost as unitid_cost,1.0 as unitid_cost_rate,'$date' as `date` from t3
               |union
               |select '可使用ocpc' as tag,ocpc_unitid_num as unitid_num,ocpc_unitid_rate as unitid_num_rate,ocpc_cost as unitid_cost,ocpc_cost_rate as unitid_cost_rate,'$date' as `date` from t3
               |union
               |select 'cv未达标' as tag,cv_unitid_num as unitid_num,cv_unitid_rate as unitid_num_rate,cv_cost as unitid_cost,cv_cost_rate as unitid_cost_rate,'$date' as `date` from t3
               |union
               |select 'auc未达标' as tag,auc_unitid_num as unitid_num,auc_unitid_rate as unitid_num_rate,auc_cost as unitid_cost,auc_cost_rate as unitid_cost_rate,'$date' as `date` from t3
               |union
               |select 'calc未达标' as tag,calc_unitid_num as unitid_num,calc_unitid_rate as unitid_num_rate,calc_cost as unitid_cost,calc_cost_rate as unitid_cost_rate,'$date' as `date` from t3
             """.stripMargin
        val r3 = spark.sql(sqlt3)

        r3.repartition(1).write.mode("overwrite").insertInto("dl_cpc.cpc_ocpc_elds_detail_metrics")

        r3.show(10)

        val sql4 =
            s"""
               |select
               |  count(1) as total_unitid_num,
               |  sum(cost) as total_cost,
               |  sum(if(is_recommend=1 and ocpc_flag=0,1,0)) as ocpc_unitid_num,
               |  round(sum(if(is_recommend=1 and ocpc_flag=0,1,0))/count(*),6) as ocpc_unitid_rate,
               |  sum(if(is_recommend=1 and ocpc_flag=0,cost,0)) as ocpc_cost,
               |  round(sum(if(is_recommend=1 and ocpc_flag=0,cost,0))/sum(cost),6) as ocpc_cost_rate,
               |  sum(if(conversion<60,1,0)) as cv_unitid_num,
               |  round(sum(if(conversion<60,1,0))/count(*),6) as cv_unitid_rate,
               |  sum(if(conversion<60,cost,0)) as cv_cost,
               |  round(sum(if(conversion<60,cost,0))/sum(cost),6) as cv_cost_rate,
               |  sum(if(auc<0.65,1,0)) as auc_unitid_num,
               |  round(sum(if(auc<0.65,1,0))/count(*),6) as auc_unitid_rate,
               |  sum(if(auc<0.65,cost,0)) as auc_cost,
               |  round(sum(if(auc<0.65,cost,0))/sum(cost),6) as auc_cost_rate,
               |  sum(if(cal_bid/acb<0.7 or cal_bid/acb>1.3,1,0)) as calc_unitid_num,
               |  round(sum(if(cal_bid/acb<0.7 or cal_bid/acb>1.3,1,0))/count(*),6) as calc_unitid_rate,
               |  sum(if(cal_bid/acb<0.7 or cal_bid/acb>1.3,cost,0)) as calc_cost,
               |  round(sum(if(cal_bid/acb<0.7 or cal_bid/acb>1.3,cost,0))/sum(cost),6) as calc_cost_rate
               |from
               |(
               |  select unitid,
               |    round(sum(cost)/count(*),6) as cost,
               |    round(sum(cvrcnt)/count(*),6) as conversion,
               |    round(sum(auc)/count(*),6) as auc,
               |    round(sum(cal_bid)/count(*),6) as cal_bid,
               |    round(sum(acb)/count(*),6) as acb,
               |    round(sum(is_recommend)/count(*),6) as is_recommend,
               |    round(sum(ocpc_flag)/count(*),6) as ocpc_flag
               |  from dl_cpc.ocpc_suggest_cpa_recommend_hourly
               |  where `date`='$date'
               |  and original_conversion = 2
               |  and industry = 'feedapp'
               |  group by unitid
               |) x
               |join
               |(
               |  select distinct unitid
               |  from union
               |) y
               |on x.unitid = y.unitid
             """.stripMargin

        val t4 = spark.sql(sql4)
        t4.createOrReplaceTempView("t4")

        val sqlt4 =
            s"""
               |select 'app api总体' as tag,total_unitid_num as unitid_num,1.0 as unitid_num_rate,total_cost as unitid_cost,1.0 as unitid_cost_rate,'$date' as `date` from t4
               |union
               |select '可使用ocpc' as tag,ocpc_unitid_num as unitid_num,ocpc_unitid_rate as unitid_num_rate,ocpc_cost as unitid_cost,ocpc_cost_rate as unitid_cost_rate,'$date' as `date` from t4
               |union
               |select 'cv未达标' as tag,cv_unitid_num as unitid_num,cv_unitid_rate as unitid_num_rate,cv_cost as unitid_cost,cv_cost_rate as unitid_cost_rate,'$date' as `date` from t4
               |union
               |select 'auc未达标' as tag,auc_unitid_num as unitid_num,auc_unitid_rate as unitid_num_rate,auc_cost as unitid_cost,auc_cost_rate as unitid_cost_rate,'$date' as `date` from t4
               |union
               |select 'calc未达标' as tag,calc_unitid_num as unitid_num,calc_unitid_rate as unitid_num_rate,calc_cost as unitid_cost,calc_cost_rate as unitid_cost_rate,'$date' as `date` from t4
             """.stripMargin
        val r4 = spark.sql(sqlt4)

        r4.repartition(1).write.mode("overwrite").insertInto("dl_cpc.cpc_ocpc_app_api_detail_metrics")

        r4.show(10)

        val sql5 =
            s"""
               |select count(1) as userid_num,
               |    sum(if(api_cost>0,1,0)) as api_userid_num,
               |    round(sum(if(api_cost>0,1,0))/count(*),6) as api_userid_rate,
               |    sum(cost) as userid_cost,
               |    sum(api_cost) as api_userid_cost,
               |    round(sum(api_cost)/sum(cost),6) as api_userid_cost_rate
               |from
               |(
               |  select userid,sum(price) as cost,sum(if(is_api_callback=1,price,0)) as api_cost
               |  from dl_cpc.slim_union_log
               |  where dt = '$date'
               |  and media_appsid in ('80000001', '80000002')
               |  and isclick=1
               |  and antispam = 0
               |  and ideaid > 0
               |  and adsrc = 1
               |  and adslot_type in (1,2,3)
               |  and industry = 'feedapp'
               |  group by userid
               |) x
             """.stripMargin

        val t5 = spark.sql(sql5)

        t5.createOrReplaceTempView("t5")

        val sqlt5 =
            s"""
               |select 'app' as tag, userid_num as userid_num, 1.0 as userid_rate, userid_cost as userid_cost, 1.0 as userid_cost_rate, '$date' as `date` from t5
               |union
               |select 'app api' as tag, api_userid_num as userid_num, api_userid_rate as userid_rate, api_userid_cost as userid_cost, api_userid_cost_rate as userid_cost_rate, '$date' as `date` from t5
             """.stripMargin

        val r5 = spark.sql(sqlt5)

        r5.repartition(1).write.mode("overwrite").insertInto("dl_cpc.cpc_ocpc_app_detail_metrics")

        r5.show(10)
    }
}

/*
create table if not exists dl_cpc.cpc_ocpc_elds_metrics
(
    tag string,
    userid_num int,
    userid_rate double
)
PARTITIONED BY (`date` string)
STORED AS PARQUET;

create table if not exists dl_cpc.cpc_ocpc_app_api_metrics
(
    tag string,
    userid_num int,
    userid_rate double
)
PARTITIONED BY (`date` string)
STORED AS PARQUET;

create table if not exists dl_cpc.cpc_ocpc_elds_detail_metrics
(
    tag string,
    unitid_num int,
    unitid_num_rate double,
    unitid_cost int,
    unitid_cost_rate double
)
PARTITIONED BY (`date` string)
STORED AS PARQUET;

create table if not exists dl_cpc.cpc_ocpc_app_api_detail_metrics
(
    tag string,
    unitid_num int,
    unitid_num_rate double,
    unitid_cost int,
    unitid_cost_rate double
)
PARTITIONED BY (`date` string)
STORED AS PARQUET;

create table if not exists dl_cpc.cpc_ocpc_app_detail_metrics
(
    tag string,
    userid_num int,
    userid_num_rate double,
    userid_cost int,
    userid_cost_rate double
)
PARTITIONED BY (`date` string)
STORED AS PARQUET;
 */
