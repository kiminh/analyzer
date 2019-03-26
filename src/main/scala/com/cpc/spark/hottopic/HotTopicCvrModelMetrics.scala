package com.cpc.spark.hottopic

import org.apache.spark.sql.SparkSession
import com.cpc.spark.tools.CalcMetrics
/**
  * @author Jinbao
  * @date 2019/3/26 9:49
  */
object HotTopicCvrModelMetrics {
    def main(args: Array[String]): Unit = {
        val day = args(0)
        val hour = args(1)
        val spark = SparkSession.builder()
          .appName(s"HotTopicCvrModelMetrics day = $day, hour = $hour")
          .enableHiveSupport()
          .getOrCreate()
        import spark.implicits._

        val sql =
            s"""
               |select cvr_model_name,exp_cvr as score,uid,if(b.searchid is not null,1,0) as label
               |from
               |(
               |    select searchid,cvr_model_name,exp_cvr,uid
               |    from dl_cpc.cpc_hot_topic_basedata_union_events
               |    where day = '$day' and hour = '$hour'
               |    and media_appsid in ('80002819')
               |    and adsrc = 1
               |    and isclick = 1
               |    and (charge_type is null or charge_type=1)
               |    and uid not like "%.%"
               |    and uid not like "%000000%"
               |    and length(uid) in (14, 15, 36)
               |    and ideaid > 0
               |    and userid > 0
               |) a
               |left outer join
               |(
               |    select tmp.searchid
               |    from
               |    (
               |        select
               |            final.searchid as searchid,
               |            final.ideaid as ideaid,
               |            case when final.src="elds" and final.label_type=6 then 1
               |                when final.src="feedapp" and final.label_type in (4, 5) then 1
               |                when final.src="yysc" and final.label_type=12 then 1
               |                when final.src="wzcp" and final.label_type in (1, 2, 3) then 1
               |                when final.src="others" and final.label_type=6 then 1
               |                else 0
               |            end as isreport
               |        from
               |        (
               |            select searchid, media_appsid, uid,
               |                planid, unitid, ideaid, adclass,
               |                case when (adclass like '134%' or adclass like '107%') then "elds"
               |                    when (adslot_type<>7 and adclass like '100%') then "feedapp"
               |                    when (adslot_type=7 and adclass like '100%') then "yysc"
               |                    when adclass in (110110100, 125100100) then "wzcp"
               |                    else "others"
               |                end as src,
               |                label_type
               |            from
               |                dl_cpc.ml_cvr_feature_v1
               |            where
               |                `date`='$day' and hour = '$hour'
               |                and label2=1
               |                and media_appsid in ("80002819")
               |            ) final
               |        ) tmp
               |    where tmp.isreport=1
               |) b
               |on a.searchid = b.searchid
               |where cvr_model_name is not null
               |and length(cvr_model_name) > 0
               |and cvr_model_name not like '%noctr%'
             """.stripMargin

        val union = spark.sql(sql).cache()

        val cvr_model_names = union.select("cvr_model_name")
          .distinct()
          .collect()
          .map(_.getAs[String]("cvr_model_name"))

        for (cvr_model_name <- cvr_model_names) {
            val modelData = union.filter(s"cvr_model_name = '$cvr_model_name'")
            val auc = CalcMetrics.getAuc(spark,modelData)
            val aucArray = CalcMetrics.getGauc(spark,modelData,"uid").filter("auc != -1").collect()
            val gauc = if(aucArray.length > 0) {
                val aucs = aucArray.map(x =>
                    (x.getAs[Double]("auc") * x.getAs[Double]("sum"), x.getAs[Double]("sum"))
                )
                .reduce((x, y) => (x._1 + y._1, x._2 + y._2))

                if (aucs._2 != 0)
                    aucs._1 * 1.0 / aucs._2
                else 0

            }
            else 0

            println(s"cvr_model_name = $cvr_model_name , auc = $auc , gauc = $gauc")
        }

    }
}
