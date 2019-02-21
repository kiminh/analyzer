package com.cpc.spark.hottopic

import com.cpc.spark.tools.CalcMetrics
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * @author Jinbao
  * @date 2019/2/21 10:32
  */
object HotTopicIdeaMetrics {
    def main(args: Array[String]): Unit = {
        val date = args(0)
        val spark = SparkSession.builder()
          .appName(s"HotTopicIdeaMetrics date = $date")
          .enableHiveSupport()
          .getOrCreate()
        import spark.implicits._
        val sql =
            s"""
               |select ext['exp_ctr'].int_value as score,
               |  isclick as label,
               |  cast(ideaid as string) as ideaid
               |from dl_cpc.cpc_hot_topic_union_log
               |where `date` = '$date'
               |and media_appsid in ('80002819')
               |and adsrc = 1
               |and isshow = 1
               |and ideaid > 0
               |and userid > 0
               |and (ext["charge_type"] IS NULL OR ext["charge_type"].int_value = 1)
             """.stripMargin

        val union = spark.sql(sql).cache()

        union.createOrReplaceTempView("union")

        val ideaidAuc = CalcMetrics.getGauc(spark,union,"ideaid").rdd
          .map(x => {
              val ideaid = x.getAs[String]("name")
              val auc = x.getAs[Double]("auc")
              val sum = x.getAs[Double]("sum")
              IdeaidAuc(ideaid,auc,sum)
              //(ideaid,auc,sum)
          })
          .collect()
          .toList
          .toDF()

        val sql2 =
            s"""
               |select ideaid,sum(label)/count(*) as ctr
               |from union
               |group by ideaid
             """.stripMargin

        val ideaidCtr = spark.sql(sql2)

//        val result = spark.sql(sql2)
//          .join(ideaidAuc,Seq("ideaid"))
//          .select("ideaid","ctr","auc")

        val sql3 =
            s"""
               |select cast(id as string) as ideaid, tokens
               |from dl_cpc.ideaid_title
             """.stripMargin

        val ideaidTitle = spark.sql(sql3)

        val result = ideaidCtr.join(ideaidAuc,Seq("ideaid")).join(ideaidTitle,Seq("ideaid"),"left_outer")
            .select("ideaid","ctr","auc","sum","tokens")

        //result.show(10)

        //result.repartition(10).write.saveAsTable("test.adlog20190221")

        result.createOrReplaceTempView("result")

        val sql4 =
            s"""
               |select cast(ideaid as bigint) as ideaid,
               |  case when ctr>0.08 then 1
               |       when ctr>0.05 then 2
               |       else 0
               |  end as ctr,
               |  case when sum>100 and auc>0.78 then 1
               |       when sum>100 and auc>0.65 then 2
               |       else 0
               |  end as auc,
               |  case when sum>100000 then 1
               |       when sum>10000  then 2
               |       when sum>1000   then 3
               |       when sum>100    then 4
               |       else 0
               |  end as show_num,
               |  tokens,
               |  '$date' as `date`
               |from result
             """.stripMargin

        spark.sql(sql4)
          .repartition(1)
          .write
          .mode("overwrite")
          .insertInto("dl_cpc.cpc_hot_topic_idea_ad_log")
    }
    case class IdeaidAuc(var ideaid:String,var auc:Double,var sum:Double)
}

/*
create table if not exists dl_cpc.cpc_hot_topic_idea_ad_log
(
    ideaid bigint,
    ctr int,
    auc int,
    show_num int,
    tokens string
)
PARTITIONED BY (`date` string)
STORED AS PARQUET;
 */
