package com.cpc.spark.coin

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col

/**
  * @author Jinbao
  * @date 2018/11/6 15:22
  */
object AutoPutCoin {
    def main(args: Array[String]): Unit = {

        val date = args(0)

        val hour = args(1).toInt

        val minute = 0

        val p = 0.7

        val spark = SparkSession.builder()
          .appName(s"AutoPutCoin date = $date, hour = $hour")
          .enableHiveSupport()
          .getOrCreate()

        val datehourlist = scala.collection.mutable.ListBuffer[String]()
        val cal = Calendar.getInstance()
        cal.set(date.substring(0, 4).toInt, date.substring(5, 7).toInt - 1, date.substring(8, 10).toInt, hour, minute)
        for (t <- 0 to 24*7) {
            if (t > 0) {
                cal.add(Calendar.HOUR, -1)
            }
            val sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            val dd = sf.format(cal.getTime())
            val d1 = dd.substring(0, 10)
            val h1 = dd.substring(11, 13)
            val datecond = s"`date` = '$d1' and hour = '$h1'"
            datehourlist += datecond
        }

        val datehour = datehourlist.toList.mkString(" or ")

        val apiUnionLogSql =
            s"""
               |select ideaid,ext["exp_cvr"].int_value as exp_cvr
               |from dl_cpc.cpc_api_union_log
               |where ($datehour)
               |and iscvr = 1
               |and media_appsid in ('80000001','80000002')
               |and ideaid > 0
               |and adslot_type in (1, 2, 3)
             """.stripMargin

        val apiUnionLog = spark.sql(apiUnionLogSql)
        println("apiUnionLog 's count is " + apiUnionLog.rdd.count())
        val apiUnionNth = getNth(apiUnionLog,p)
        println("apiUnionNth 's count is " + apiUnionNth.count())
        val mlFeatureSql =
            s"""
               |select distinct searchid
               |from dl_cpc.ml_cvr_feature_v1
               |where ($datehour)
               |and label2 = 1
             """.stripMargin

        val mlFeature = spark.sql(mlFeatureSql)
        println("mlFeature 's count is " + mlFeature.rdd.count())
        val unionLogSql =
            s"""
               |select searchid, ideaid,ext["exp_cvr"].int_value as exp_cvr1
               |from dl_cpc.cpc_union_log
               |where ($datehour)
               |and media_appsid in ('80000001','80000002')
               |and ideaid > 0
               |and adslot_type in (1, 2, 3)
             """.stripMargin

        val unionLog = spark.sql(unionLogSql)
        println("unionLog 's count is " + unionLog.rdd.count())
        val cols = Array("ideaid", "exp_cvr1")

        val unionLogFilter = unionLog.join(mlFeature,Seq("searchid"),"inner").select("ideaid", "exp_cvr1")
        //  .select(cols.map(col):_*)
        unionLogFilter.show(2)
        println("unionLogFilter 's count is " + unionLogFilter.rdd.count())
        val unionNth = getNth(unionLogFilter, p)
        println("unionNth 's count is " + unionNth.count())
        val Nth = unionNth.join(apiUnionNth)

        println("Nth 's count is " + Nth.count())
    }
    def getNth(df: DataFrame,p:Double): RDD[(Int,Int)] = {
        df.rdd.map(x => (x.getAs[Int]("ideaid"), x.getAs[Int]("exp_cvr")))
          .combineByKey(x => List(x),
            (x:List[Int], y:Int) => y::x,
            (x:List[Int], y:List[Int]) => x:::y)
            .mapValues(x => {
                val sorted = x.sorted
                val index = (x.length * p).toInt
                x(index)
            })
    }
}
