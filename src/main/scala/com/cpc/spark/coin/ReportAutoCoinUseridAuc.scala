package com.cpc.spark.coin

import com.cpc.spark.tools.CalcMetrics
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

/**
  * @author Jinbao
  * @date 2019/3/18 11:36
  */
object ReportAutoCoinUseridAuc {
    def main(args: Array[String]): Unit = {
        val date = args(0)
        val spark = SparkSession.builder()
          .appName(s"ReportAutoCoinUseridAuc date = $date")
          .enableHiveSupport()
          .getOrCreate()
        import spark.implicits._

        val sql =
            s"""
               |select cast(userid as string) as userid,
               |    cast(ideaid as string) as ideaid,
               |    exp_cvr as score,
               |    if (b.searchid is null,0,1) as label
               |from
               |(
               |    select searchid, userid, ideaid, isshow, isclick, price, uid, is_auto_coin, exp_cvr, exp_style
               |    from dl_cpc.cpc_basedata_union_events
               |    where day = '$date'
               |    and media_appsid in ("80000001", "80000002")
               |    and isshow = 1
               |    and ideaid > 0
               |    and adsrc = 1
               |    and city_level != 1
               |    and (charge_type is null or charge_type=1)
               |    and userid not in (1001028, 1501875)
               |    and adslot_id not in ("7774304","7636999","7602943","7783705","7443868","7917491","7335680","7871301")
               |    and round(adclass/1000) != 132101
               |    and adslot_type in (1,2)
               |) a
               |left outer join
               |(
               |    select tmp.searchid
               |    from
               |    (
               |        select final.searchid as searchid, final.ideaid as ideaid,
               |            case when final.src="elds" and final.label_type=6 then 1
               |                when final.src="feedapp" and final.label_type in (4, 5) then 1
               |                when final.src="yysc" and final.label_type=12 then 1
               |                when final.src="wzcp" and final.label_type in (1, 2, 3) then 1
               |                when final.src="others" and final.label_type=6 then 1
               |                else 0
               |            end as isreport
               |        from
               |        (
               |            select searchid, media_appsid, uid, planid, unitid, ideaid, adclass,
               |                case
               |                    when (adclass like '134%' or adclass like '107%') then "elds"
               |                    when (adslot_type<>7 and adclass like '100%') then "feedapp"
               |                    when (adslot_type=7 and adclass like '100%') then "yysc"
               |                    when adclass in (110110100, 125100100) then "wzcp"
               |                    else "others"
               |                end as src,
               |                label_type
               |            from dl_cpc.ml_cvr_feature_v1
               |            where `date`='$date'
               |                and label2=1
               |                and media_appsid in ("80000001", "80000002")
               |            ) final
               |        ) tmp
               |    where tmp.isreport=1
               |) b
               |on a.searchid = b.searchid
             """.stripMargin

        val useridAucList = spark.sql(sql)

        val uAuc = CalcMetrics.getGauc(spark,useridAucList,"userid")
          .withColumnRenamed("name","userid")
          .withColumn("date",lit(date))
          .select("userid","auc","date")

        uAuc.repartition(1)
          .write.insertInto("dl_cpc.cpc_coin_userid_auc")

        val iAuc = CalcMetrics.getGauc(spark,useridAucList,"ideaid")
          .withColumnRenamed("name","ideaid")
          .withColumn("date",lit(date))
          .select("ideaid","auc","date")

        iAuc.repartition(1)
          .write.insertInto("dl_cpc.cpc_coin_ideaid_auc")

    }
}

/*

create table if not exists dl_cpc.cpc_coin_userid_auc
(
    userid string,
    auc double
) PARTITIONED BY (`date` string)
STORED as PARQUET;

create table if not exists dl_cpc.cpc_coin_ideaid_auc
(
    ideaid string,
    auc double
) PARTITIONED BY (`date` string)
STORED as PARQUET;
 */
