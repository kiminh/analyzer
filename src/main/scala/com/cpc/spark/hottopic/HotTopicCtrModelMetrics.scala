package com.cpc.spark.hottopic

import com.cpc.spark.tools.CalcMetrics
import org.apache.spark.sql.SparkSession

/**
  * @author Jinbao
  * @date 2019/2/14 11:04
  */
object HotTopicCtrModelMetrics {
    def main(args: Array[String]): Unit = {
        val date = args(0)
        val spark = SparkSession.builder()
          .appName(s"HotTopicCtrModelMetrics date = $date")
          .enableHiveSupport()
          .getOrCreate()
        import spark.implicits._

        val sql1 =
            s"""
               |select
               |  ext_string['ctr_model_name'] as ctr_model_name,
               |  sum(isshow) as show_num,
               |  sum(isclick) as click_num,
               |  round(sum(isclick)/sum(isshow),6) as ctr,
               |  round(sum(ext['exp_ctr'].int_value/1000000)/sum(isshow),6) as exp_ctr,
               |  round(sum(ext['exp_ctr'].int_value/1000000)/sum(isshow),6)/round(sum(isclick)/sum(isshow),6) as pcoc,
               |  sum(case WHEN isclick = 1 then price else 0 end) as click_total_price,
               |  round(sum(case WHEN isclick = 1 then price else 0 end)*10/sum(isshow), 6) as cpc_cpm,
               |  round(sum(case WHEN isclick = 1 then price else 0 end)*10/count(distinct uid), 6) as cpc_arpu,
               |  '$date' as `date`
               |from dl_cpc.cpc_hot_topic_union_log
               |where `date` = '$date'
               |and media_appsid = '80002819'
               |and adsrc = 1
               |and isshow = 1
               |and ideaid>0
               |and isshow=1
               |and uid not like "%.%"
               |and uid not like "%000000%"
               |and length(uid) in (14, 15, 36)
               |and userid > 0
               |AND (ext["charge_type"] is null or ext["charge_type"].int_value=1)
               |and ext['adclass'].int_value != 132102100
               |group by ext_string['ctr_model_name']
             """.stripMargin

        println(sql1)

        val m1 = spark.sql(sql1)

        val sql2 =
            s"""
               |select ext['exp_ctr'].int_value as score,
               |  isclick as label,
               |  ext_string['ctr_model_name'] as ctr_model_name
               |from dl_cpc.cpc_hot_topic_union_log
               |where `date` = '$date'
               |and media_appsid = '80002819'
               |and adsrc = 1
               |and isshow = 1
               |and ideaid>0
               |and isshow=1
               |and uid not like "%.%"
               |and uid not like "%000000%"
               |and length(uid) in (14, 15, 36)
               |and userid > 0
               |AND (ext["charge_type"] is null or ext["charge_type"].int_value=1)
               |and ext['adclass'].int_value != 132102100
             """.stripMargin

        val union = spark.sql(sql2)

        val result = CalcMetrics.getGauc(spark,union,"ctr_model_name")
          .withColumnRenamed("name","ctr_model_name")
          .select("ctr_model_name","auc")
          .join(m1,Seq("ctr_model_name"))
          .select("ctr_model_name","show_num","click_num","ctr","exp_ctr","pcoc","click_total_price","cpc_cpm","cpc_arpu","auc","date")

        result.show(10)

    }
}
