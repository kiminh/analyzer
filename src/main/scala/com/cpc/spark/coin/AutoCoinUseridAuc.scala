package com.cpc.spark.coin

import com.cpc.spark.tools.CalcMetrics
import org.apache.spark.sql.SparkSession

/**
  * @author Jinbao
  * @date 2019/1/14 14:23
  */
object AutoCoinUseridAuc {
    def main(args: Array[String]): Unit = {
        val date = args(0)
        val spark = SparkSession.builder()
          .appName(s"AutoCoinUseridAuc date = $date")
          .enableHiveSupport()
          .getOrCreate()
        import spark.implicits._

        val unionSql =
            s"""
               |select b.label2 as label,
               |  a.ext['exp_cvr'].int_value as score,
               |  cast(a.userid as string) as userid,
               |  a.adslot_type as adslot_type
               |from
               |    (
               |        select *
               |        from dl_cpc.cpc_union_log
               |        where `date`='$date'
               |        and media_appsid  in ("80000001", "80000002") and isclick = 1
               |        and ext['antispam'].int_value = 0 and ideaid > 0
               |        and adsrc = 1
               |        and ext['city_level'].int_value != 1
               |        AND (ext["charge_type"] IS NULL OR ext["charge_type"].int_value = 1)
               |        and userid not in (1001028, 1501875)
               |        and adslotid not in ("7774304","7636999","7602943","7783705","7443868","7917491","7868332")
               |        and round(ext["adclass"].int_value/1000) != 132101
               |        and adslot_type in (1,2)
               |    ) a
               |    left outer join
               |    (
               |        select searchid, label2
               |        from dl_cpc.ml_cvr_feature_v1
               |        where `date`='$date'
               |    ) b
               |    on a.searchid = b.searchid
             """.stripMargin

        val data = spark.sql(unionSql).cache()
        val resultListBuffer = scala.collection.mutable.ListBuffer[AucUid]()
        for (adslot_type <- 1 to 2) {
            val dataFilter = data.filter(s"adslot_type = $adslot_type")

            if (dataFilter.count()>0) {
                val aucList = CalcMetrics.getGauc(spark,dataFilter,"userid").collect()
                aucList.foreach(x => {
                    val userid = x.getAs[String]("name")
                    val auc = x.getAs[Double]("auc")
                    resultListBuffer += AucUid(userid = userid,
                        auc = auc,
                        adslot_type = adslot_type,
                        date = date)
                })

            }
        }
        val result = resultListBuffer.toList.toDF()
        result.repartition(1)
          .write
          .mode("overwrite")
          .insertInto("dl_cpc.cpc_qtt_cvr_userid_auc")
    }
    case class AucUid(var userid:String = "",
                      var auc:Double = 0,
                      var adslot_type:Int = 0,
                      var date:String = "")
}
