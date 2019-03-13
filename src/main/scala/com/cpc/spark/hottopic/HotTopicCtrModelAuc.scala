package com.cpc.spark.hottopic

import com.cpc.spark.tools.CalcMetrics
import org.apache.spark.sql.SparkSession

/**
  * @author Jinbao
  * @date 2019/3/7 15:32
  */
object HotTopicCtrModelAuc {
    def main(args: Array[String]): Unit = {
        val day = args(0)
        val hour = args(1)

        val spark = SparkSession.builder()
          .appName(s"HotTopicCtrModelAuc day = $day, hour = $hour")
          .enableHiveSupport()
          .getOrCreate()

        import spark.implicits._

        val sql =
            s"""
               |select exp_ctr as score,
               |  isclick as label,
               |  ctr_model_name
               |from dl_cpc.cpc_hot_topic_basedata_union_events
               |where day = '$day' and hour = '$hour'
               |and media_appsid in ('80002819')
               |and adsrc = 1
               |and isshow = 1
               |and ideaid > 0
               |and userid > 0
               |and uid not like "%.%"
               |and uid not like "%000000%"
               |and length(uid) in (14, 15, 36)
               |and (charge_type is null or charge_type = 1)
               |and adclass not in (132102100)
               |and length(ctr_model_name) > 0
             """.stripMargin



        val data = spark.sql(sql).cache()

        val ctrModelNames = data.select("ctr_model_name")
          .distinct()
          .collect()
          .map(x => x.getAs[String]("ctr_model_name"))

        for (ctrModelName <- ctrModelNames) {
            val ctrModelUnion = data.filter(s"ctr_model_name = '$ctrModelName'")
            val ctrModelAuc = CalcMetrics.getAuc(spark, ctrModelUnion)
            val ctrModelGauc = CalcMetrics.getGauc(spark,ctrModelUnion, "uid")
              .filter(x => x.getAs[Double]("auc") != -1)
              .map(x => (x.getAs[Double]("auc") * x.getAs[Double]("sum"), x.getAs[Double]("sum")))
              .reduce((x, y) => (x._1 + y._1, x._2 + y._2))

        }

    }
}
