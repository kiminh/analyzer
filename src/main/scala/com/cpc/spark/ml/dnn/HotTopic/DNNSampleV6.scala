package com.cpc.spark.ml.dnn.HotTopic

import com.cpc.spark.ml.dnn.DNNSample
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.array

/**
  * @author Jinbao
  * @date 2018/12/18 19:36
  */
object DNNSampleV6 {
    def main(args: Array[String]): Unit = {
        val Array(trdate, trpath, tedate, tepath) = args

        val spark = SparkSession.builder()
          .appName(s"HotTopic DNNSampleV6 date = $trdate")
          .enableHiveSupport()
          .getOrCreate()

        import spark.implicits._
    }
}

class DNNSampleV6(spark: SparkSession, trdate: String = "", trpath: String = "",
                  tedate: String = "", tepath: String = "")
  extends DNNSample(spark, trdate, trpath, tedate, tepath) {
    /**
      * as features：id 类特征的 one hot feature    ====== 前缀 f+index+"#" 如 f0#,f1#,f2#..
      *
      * @param date
      * @param adtype
      * @return
      */
    private def getAsFeature(date: String, adtype: Int = 1): DataFrame = {
        import spark.implicits._
        val as_sql =
            s"""
               |select a.searchid,
               |  if(coalesce(c.label, b.label, 0) > 0, array(1, 0), array(0, 1)) as cvr_label,
               |  if(a.isclick>0, array(1,0), array(0,1)) as label,
               |  a.media_type, a.media_appsid as mediaid,
               |  a.ext['channel'].int_value as channel,
               |  a.ext['client_type'].string_value as sdk_type,
               |
               |  a.adslot_type, a.adslotid,
               |
               |  a.adtype, a.interaction, a.bid, a.ideaid, a.unitid, a.planid, a.userid,
               |  a.ext_int['is_new_ad'] as is_new_ad, a.ext['adclass'].int_value as adclass,
               |  a.ext_int['siteid'] as site_id,
               |
               |  a.os, a.network, a.ext['phone_price'].int_value as phone_price,
               |  a.ext['brand_title'].string_value as brand,
               |
               |  a.province, a.city, a.ext['city_level'].int_value as city_level,
               |
               |  a.uid, a.age, a.sex, a.ext_string['dtu_id'] as dtu_id,
               |
               |  a.hour, a.ext_int['content_id'] as content_id,
               |  a.ext_int['category'] as content_category
               |
               |from dl_cpc.cpc_union_log a
               |left join dl_cpc.ml_cvr_feature_v1 b
               |  on a.searchid=b.searchid
               |  and b.label2=1
               |  and b.date='$date'
               |left join dl_cpc.ml_cvr_feature_v2 c
               |  on a.searchid=c.searchid
               |  and c.label=1
               |  and c.date='$date'
               |where a.`date` = '$date'
               |  and a.isshow = 1 and a.ideaid > 0 and a.adsrc = 1
               |  and a.media_appsid in ("80002819")
               |  and a.uid not like "%.%"
               |  and a.uid not like "%000000%"
               |  and length(a.uid) in (14, 15, 36)
            """.stripMargin
        println("============= as features ==============")
        println(as_sql)

        val data = spark.sql(as_sql).persist()

        data.write.mode("overwrite").parquet(s"/user/cpc/dnn/raw_data_list/$date")

        data
          .select($"label",
              $"uid",
              $"ideaid",
              hash("f0#")($"media_type").alias("f0"),
              hash("f1#")($"mediaid").alias("f1"),
              hash("f2#")($"channel").alias("f2"),
              hash("f3#")($"sdk_type").alias("f3"),
              hash("f4#")($"adslot_type").alias("f4"),
              hash("f5#")($"adslotid").alias("f5"),
              hash("f6#")($"sex").alias("f6"),
              hash("f7#")($"dtu_id").alias("f7"),
              hash("f8#")($"adtype").alias("f8"),
              hash("f9#")($"interaction").alias("f9"),
              hash("f10#")($"bid").alias("f10"),
              hash("f11#")($"ideaid").alias("f11"),
              hash("f12#")($"unitid").alias("f12"),
              hash("f13#")($"planid").alias("f13"),
              hash("f14#")($"userid").alias("f14"),
              hash("f15#")($"is_new_ad").alias("f15"),
              hash("f16#")($"adclass").alias("f16"),
              hash("f17#")($"site_id").alias("f17"),
              hash("f18#")($"os").alias("f18"),
              hash("f19#")($"network").alias("f19"),
              hash("f20#")($"phone_price").alias("f20"),
              hash("f21#")($"brand").alias("f21"),
              hash("f22#")($"province").alias("f22"),
              hash("f23#")($"city").alias("f23"),
              hash("f24#")($"city_level").alias("f24"),
              hash("f25#")($"uid").alias("f25"),
              hash("f26#")($"age").alias("f26"),
              hash("f27#")($"hour").alias("f27")
          )
          .select(
              array($"f0", $"f1", $"f2", $"f3", $"f4", $"f5", $"f6", $"f7", $"f8", $"f9",
                  $"f10", $"f11", $"f12", $"f13", $"f14", $"f15", $"f16", $"f17", $"f18", $"f19",
                  $"f20", $"f21", $"f22", $"f23", $"f24", $"f25", $"f26", $"f27")
                .alias("dense"),
              $"label",
              $"uid",
              $"ideaid"
          ).repartition(1000, $"uid")
    }
}
