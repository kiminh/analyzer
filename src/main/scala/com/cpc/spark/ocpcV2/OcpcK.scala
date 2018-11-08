package com.cpc.spark.ocpcV2

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.commons.math3.fitting.{PolynomialCurveFitter, WeightedObservedPoints}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import org.apache.spark.sql.functions._

object OcpcK {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("ocpc v2").enableHiveSupport().getOrCreate()

    val date = args(0).toString
    val hour = args(1).toString
    // val onDuty = args(2).toInt

    val datehourlist = scala.collection.mutable.ListBuffer[String]()
    val datehourlist2 = scala.collection.mutable.ListBuffer[String]()
    val cal = Calendar.getInstance()
    cal.set(date.substring(0, 4).toInt, date.substring(5, 7).toInt - 1, date.substring(8, 10).toInt, hour.toInt, 0)
    for (t <- 0 to 24) {
      if (t > 0) {
        cal.add(Calendar.HOUR, -1)
      }
      val sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val dd = sf.format(cal.getTime())
      val d1 = dd.substring(0, 10)
      val h1 = dd.substring(11, 13)
      val datecond = s"`date` = '$d1' and hour = '$h1'"
      val datecond2 = s"`dt` = '$d1' and hour = '$h1'"
      datehourlist += datecond
      datehourlist2 += datecond2
    }

    val dtCondition = "(%s)".format(datehourlist.mkString(" or "))
    val dtCondition2 = "(%s)".format(datehourlist2.mkString(" or "))


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
    spark.sql(statSql)
      .withColumn("`date`", lit(date))
      .withColumn("hour", lit(hour))
      .write.mode("overwrite").partitionBy("`date`", "hour").saveAsTable(tablename)

    val res = spark.table(tablename).where("ratio2 is not null")
      .withColumn("str", concat_ws(" ", col("k"), col("ratio2"), col("clickCnt")))
      .groupBy("ideaid")
      .agg(collect_set("str").as("liststr"))
      .select("ideaid", "liststr").collect()

    val targetK = 0.95
    for (row <- res) {
      val ideaid = row(0).toString.toInt
      val pointList = row(1).asInstanceOf[scala.collection.mutable.WrappedArray[String]].map(x => {
        val y = x.trim.split("\\s+")
        (y(0).toDouble, y(1).toDouble, y(2).toInt)
      })
      val coffList = fitPoints(pointList.toList)
      val k = (targetK - coffList(0)) / coffList(1)
      val realk = k * 5.0 / 100.0
      println("ideaid " + ideaid, "coff " + coffList, "target k: " + k, "realk: " + realk)
    }

  }

  def fitPoints(pointsWithCount: List[(Double, Double, Int)]): List[Double] = {
    var obs: WeightedObservedPoints = new WeightedObservedPoints();
    var count = 0
    for ((x, y, n) <- pointsWithCount) {
      for (i <- 1 to n) {
        obs.add(x, y);
      }
      count = count + n
      println("sample", x, y)
    }

    for (i <- 0 to count / 5) {
      obs.add(0.0, 0.0);
    }

    // Instantiate a third-degree polynomial fitter.
    var fitter: PolynomialCurveFitter = PolynomialCurveFitter.create(1);

    var res = mutable.ListBuffer[Double]()
    // Retrieve fitted parameters (coefficients of the polynomial function).
    for (c <- fitter.fit(obs.toList)) {
      res.append(c)
    }
    res.toList
  }


}
