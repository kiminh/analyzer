package com.cpc.spark.ocpcV3.HotTopicOcpc.model_test

import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types


object mengtui {
  def main(args: Array[String]): Unit ={
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    import spark.implicits._
    val sqlRequest =
      s"""
         |select
         | adslot_type,
         | unitid,
         | ideaid,
         | exp_ctr as score_ctr,
         | isclick as label_ctr,
         | exp_cvr as score_cvr,
         | iscvr   as label_cvr
         |from (
         | select * from  dl_cpc.slim_union_log
         |where dt = '2019-03-03'
         |  and adsrc = 1
         |  and userid >0
         |  and isshow = 1
         |  and antispam = 0
         |  and (charge_type is NULL or charge_type = 1)
         |  and media_appsid in ('80000001', '80000002') --qtt
         |  and ideaid in (2640880, 2734591, 2734594, 2753214)
         |   ) t1
         |left join (
         |  select
         |    searchid,
         |    label2 as iscvr --是否转化
         |  from dl_cpc.ml_cvr_feature_v1
         | WHERE `date` = '2019-03-03'
         |) t2
         |on t1.searchid = t2.searchid
       """.stripMargin

    val df = spark.sql(sqlRequest)
    var result: List[IdeaAcu] = List()
    var result2: List[IdeaAcu] = List()
    var result_comb: List[IdeaAcu] = List()

    for(ideaid <- List(2640880, 2734591, 2734594, 2753214)){
      val df1 = df.filter(s"ideaid = $ideaid")
        .withColumn("score", col("score_ctr").cast(types.LongType))
        .withColumn("label", col("label_ctr").cast(types.IntegerType))
        .select("ideaid", "score", "label")
      val auc = getAuc(spark, df1)

      val df2 = df.filter(s"ideaid = $ideaid and isclick = 1")
        .withColumn("score", col("score_cvr").cast(types.LongType))
        .withColumn("label", col("label_cvr").cast(types.IntegerType))
        .select("ideaid", "score", "label")
      val auc2 = getAuc(spark, df2)
      result = IdeaAcu(ideaid, "ctr", auc)::result
      result2 = IdeaAcu(ideaid, "cvr", auc2)::result2
    }

    val df_ctr = df
        .withColumn("score", col("score_ctr").cast(types.LongType))
        .withColumn("label", col("label_ctr").cast(types.IntegerType))
        .select("ideaid", "score", "label")
    val auc_ctr = getAuc(spark, df_ctr)


    val df_cvr = df
      .withColumn("score", col("score_cvr").cast(types.LongType))
      .withColumn("label", col("label_cvr").cast(types.IntegerType))
      .select("ideaid", "score", "label")
    val auc_cvr = getAuc(spark, df_cvr)
    result_comb = IdeaAcu(100, "ctr", auc_ctr)::result_comb
    result_comb = IdeaAcu(100, "cvr", auc_cvr)::result_comb

    result.toDS().show()
    result2.toDS().show()
    result_comb.toDS().show()
  }

  def getAuc(spark:SparkSession, data:DataFrame): Double = {
    import spark.implicits._
    val scoreAndLable = data.select($"score",$"label")
      .rdd
      .map(x => (x.getAs[Long]("score").toDouble,
        x.getAs[Int]("label").toDouble))
    val metrics = new BinaryClassificationMetrics(scoreAndLable)
    val aucROC = metrics.areaUnderROC
    aucROC
  }
  case class IdeaAcu(idea: Int, cat: String, auc: Double)

}
