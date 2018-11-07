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

        val preDay = 3

        val spark = SparkSession.builder()
          .appName(s"AutoPutCoin date = $date, hour = $hour")
          .enableHiveSupport()
          .getOrCreate()

        import spark.implicits._

        val datehourlist = scala.collection.mutable.ListBuffer[String]()
        val cal = Calendar.getInstance()
        cal.set(date.substring(0, 4).toInt, date.substring(5, 7).toInt - 1, date.substring(8, 10).toInt, hour, minute)
        for (t <- 0 to 24 * preDay) {
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
               |select ideaid,exp_cvr
               |from test.ml_cvr_feature_v1
               |where ($datehour)
               |and label2 = 1
               |and media_appsid in ('80000001','80000002')
               |and adslot_type in (1, 2, 3)
               |and ideaid > 0
             """.stripMargin

        val mlFeature = spark.sql(mlFeatureSql)
        println("mlFeature 's count is " + mlFeature.rdd.count())

        val mlFeatureNth = getNth(mlFeature, p)


        val Nth = mlFeatureNth.fullOuterJoin(apiUnionNth)
            .map(x => coin(ideaid = x._1,
                label_exp_cvr = x._2._1._1,
                label_min = x._2._1._2,
                label_max = x._2._1._3,
                label_num = x._2._1._4,
                label_5th = x._2._1._5,
                label_6th = x._2._1._6,
                label_7th = x._2._1._7,
                label_8th = x._2._1._8,
                label_9th = x._2._1._9,

                api_exp_cvr = x._2._2._1,
                api_min = x._2._2._2,
                api_max = x._2._2._3,
                api_num = x._2._2._4,
                api_5th = x._2._2._5,
                api_6th = x._2._2._6,
                api_7th = x._2._2._7,
                api_8th = x._2._2._8,
                api_9th = x._2._2._9,

                date = date,
                hour = hour.toString)
            )
          .toDS()

        Nth.write.mode("overwrite").insertInto("test.coin2")

        println("Nth 's count is " + Nth.count())

        spark.stop()
    }
    def getNth(df: DataFrame,p:Double): RDD[(Int,(Int, Int, Int, Int, Int, Int, Int, Int, Int))] = {
        df.rdd.map(x => (x.getAs[Int]("ideaid"), x.getAs[Int]("exp_cvr")))
          .combineByKey(x => List(x),
            (x:List[Int], y:Int) => y::x,
            (x:List[Int], y:List[Int]) => x:::y)
            .mapValues(x => {
                val sorted = x.sorted
                val index = (x.length * p).toInt
                val i5th = (x.length * 0.5).toInt
                val i6th = (x.length * 0.6).toInt
                val i7th = (x.length * 0.7).toInt
                val i8th = (x.length * 0.8).toInt
                val i9th = (x.length * 0.9).toInt
                (x(index), x(0),x(x.length - 1), x.length, x(i5th), x(i6th), x(i7th), x(i8th), x(i9th))
                //x(index)
            })
    }

}
case class coin(var ideaid:Int = 0,
                var label_exp_cvr: Int = 0,
                var label_min: Int = 0,
                var label_max: Int = 0,
                var label_num: Int = 0,
                var label_5th: Int = 0,
                var label_6th: Int = 0,
                var label_7th: Int = 0,
                var label_8th: Int = 0,
                var label_9th: Int = 0,

                var api_exp_cvr: Int = 0,
                var api_min: Int = 0,
                var api_max: Int = 0,
                var api_num: Int = 0,
                var api_5th: Int = 0,
                var api_6th: Int = 0,
                var api_7th: Int = 0,
                var api_8th: Int = 0,
                var api_9th: Int = 0,
                var date:String = "",
                var hour:String = "")