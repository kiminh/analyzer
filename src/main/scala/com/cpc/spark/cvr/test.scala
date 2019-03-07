package com.cpc.spark.cvr

import com.cpc.spark.tools.CalcMetrics
import org.apache.spark.sql.SparkSession

/**
  * @author Jinbao
  * @date 2019/3/7 17:53
  */
object test {
    def main(args: Array[String]): Unit = {
        val date = args(0)

        val spark = SparkSession.builder()
          .appName(s"test date = $date")
          .enableHiveSupport()
          .getOrCreate()
        import spark.implicits._

        val sql =
            s"""
               |select
               | ideaid,
               | exp_ctr as score_ctr,
               | isclick as label_ctr,
               | exp_cvr as score_cvr,
               | iscvr   as label_cvr
               |from (
               |  select ideaid,exp_ctr,isclick,exp_cvr
               |  from  dl_cpc.slim_union_log
               |  where dt = '2019-03-03'
               |  and adsrc = 1
               |  and userid >0
               |  and isshow = 1
               |  and antispam = 0
               |  and (charge_type is NULL or charge_type = 1)
               |  and media_appsid in ('80000001', '80000002') --qtt
               |  and ideaid in (2640880, 2734591, 2734594, 2753214)
               |) t1
               |left join (
               |  select
               |    searchid,
               |    label2 as iscvr --是否转化
               |  from dl_cpc.ml_cvr_feature_v1
               | WHERE `date` = '2019-03-03'
               |) t2
               |on t1.searchid = t2.searchid
             """.stripMargin

        val data = spark.sql(sql).cache()

        val ideaid = data.select("ideaid").rdd.map(_.getAs[Int]("ideaid")).collect().toList

        for (i <- ideaid) {
            val data1 = data.filter(s"ideaid = $i").cache()
              val ctr_data = data1
              .select("score_ctr","label_ctr")
              .withColumnRenamed("score_ctr","score")
              .withColumnRenamed("label_ctr","label")

            val ctr_auc = CalcMetrics.getAuc(spark, ctr_data)

            val cvr_data = data1
              .select("score_cvr","label_cvr")
              .withColumnRenamed("score_cvr","score")
              .withColumnRenamed("label_cvr","label")

            val cvr_auc = CalcMetrics.getAuc(spark, cvr_data)

            data1.unpersist()

            println(i,ctr_auc,cvr_auc)
        }
    }
}
