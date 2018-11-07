package com.cpc.spark.ocpcV2

import org.apache.commons.math3.fitting.{PolynomialCurveFitter, WeightedObservedPoints}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import org.apache.spark.sql.functions._

object OcpcK {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("ocpc v2").enableHiveSupport().getOrCreate()

    // val date = args(0).toString
    // val hour = args(1).toString
    // val onDuty = args(2).toInt

    val dtCondition = "`date` = '2018-11-05' and hour in ('15','16')"
    val dtCondition2 = "`dt` = '2018-11-05' and hour in ('15','16')"

    val statSql =
      s"""
         |select
         |  ideaid,
         |  round(ocpc_log_dict['kvalue'] * ocpc_log_dict['cali'] * 100.0 / 5) as k,
         |  ocpc_log_dict['cpagiven'] as cpagiven,
         |  sum(if(isclick=1,price,0))/sum(COALESCE(label2,0)) as cpa2,
         |  sum(if(isclick=1,price,0))/sum(COALESCE(label3,0)) as cpa3,
         |  sum(if(isclick=1,price,0))/sum(COALESCE(label2,0))/ocpc_log_dict['cpagiven'] as ratio2,
         |  sum(if(isclick=1,price,0))/sum(COALESCE(label3,0))/ocpc_log_dict['cpagiven'] as ratio3,
         |  sum(isclick) clickCnt,
         |  sum(COALESCE(label2,0)) cvr2Cnt,
         |  sum(COALESCE(label3,0)) cvr3Cnt
         |from
         |  (select * from dl_cpc.ocpc_unionlog where $dtCondition2 and ocpc_log_dict['kvalue'] is not null and isclick=1) a
         |  left outer join
         |  (select searchid, label2 from dl_cpc.ml_cvr_feature_v1 where $dtCondition) b on a.searchid = b.searchid
         |  left outer join
         |  (select searchid, iscvr as label3 from dl_cpc.cpc_api_union_log where $dtCondition) c on a.searchid = c.searchid
         |group by ideaid, round(ocpc_log_dict['kvalue'] * ocpc_log_dict['cali'] * 100.0 / 5) , ocpc_log_dict['cpagiven']
      """.stripMargin

    println(statSql)

    val tablename = "test.djq_ocpc"
    spark.sql(statSql).write.mode("overwrite").saveAsTable(tablename)

    val res = spark.table(tablename).where("ratio2 is not null")
      .withColumn("str", concat_ws(" ", col("k"), col("ratio2"), col("clickCnt")))
      .groupBy("ideaid")
      .agg(collect_set("str").as("liststr"))
      .select("ideaid", "liststr").collect()

    for (row <- res) {
      val ideaid = row(0).toString.toInt
      val pointList = row(1).asInstanceOf[scala.collection.mutable.WrappedArray[String]].map(x => {
        x.split("\\s+")
        (x(0).toDouble, x(1).toDouble, x(2).toInt)
      })
      val coffList = fitPoints(pointList.toList)
      val k = (1.0 - coffList(1)) / coffList(0)
      println("ideaid " + ideaid, "coff " + coffList, "k: " + k)
    }

  }

  def fitPoints(pointsWithCount: List[(Double, Double, Int)]): List[Double] = {
    var obs: WeightedObservedPoints = new WeightedObservedPoints();
    if (pointsWithCount.size <= 1) {
      obs.add(0.0, 0.0);
    }
    for ((x, y, n) <- pointsWithCount) {
      for (i <- 1 to n) {
        obs.add(x, y);
      }
    }

    // Instantiate a third-degree polynomial fitter.
    var fitter: PolynomialCurveFitter = PolynomialCurveFitter.create(1);


    var res = mutable.ListBuffer[Double]()
    // Retrieve fitted parameters (coefficients of the polynomial function).
    for (c <- fitter.fit(obs.toList)) {
      res.append(c)
    }
    for ((x, y, n) <- pointsWithCount) {
      println("test", y, res(0) + x * res(1))
    }
    res.toList
  }


}
