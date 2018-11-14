package com.cpc.spark.cvr

import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

/**
  * @author Jinbao
  * @date 2018/11/14 17:00
  */
object CvrRatio {
    def main(args: Array[String]): Unit = {
        val date = args(0)
        val spark = SparkSession.builder()
          .appName(s"CvrRatio date = $date")
          .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
          .enableHiveSupport()
          .getOrCreate()
        val dateList = scala.collection.mutable.ListBuffer[String]()
        val cal = Calendar.getInstance()
        cal.set(date.substring(0, 4).toInt, date.substring(5, 7).toInt - 1, date.substring(8, 10).toInt, 0, 0)
        for (t <- 0 to 2) {
            if (t > 0) {
                cal.add(Calendar.DATE, -1)
            }
            val sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            val dd = sf.format(cal.getTime())
            val d1 = dd.substring(0, 10)
            val datecond = s"`date` = '$d1'"
            dateList += datecond
        }

        val datelist = dateList.toList.mkString(" or ")

        val unionSql =
            s"""
               |select media_appsid,
               |sum(ext["exp_cvr"].int_value) / count(*) as exp_cvr
               |from dl_cpc.cpc_union_log
               |where ($datelist)
               |and media_appsid not in ('80000001','80000002')
               |and isclick = 1
               |group by media_appsid
             """.stripMargin

        val mlFeatureSql =
            s"""
               |select media_appsid,sum(label2) / count(*) as acutal_cvr
               |from dl_cpc.ml_cvr_feature_v1
               |where `date` = $date
               |and media_appsid not in ('80000001','80000002')
               |group by media_appsid
             """.stripMargin

        val union = spark.sql(unionSql)
        union.show()
        val mlFeature = spark.sql(mlFeatureSql)
        mlFeature.show()
        val result = union.join(mlFeature,Seq("media_appsid"),"inner")
          .withColumn("ratio",calRatio(col("exp_cvr"),col("acutal_cvr")))
          .withColumn("date", lit(s"$date"))
          .cache()

        result.show()
        //val r = result.collect()
//        println("result 's count is " + r.length)
//        result.coalesce(1)
//          .write.mode("overwrite")
//          .insertInto("dl_cpc.cvrratio")
//        println("insert into dl_cpc.cvrratio success!")
    }
    def calRatio = udf((exp_cvr:Int, acutal_cvr:Int) => {
        1.0 * acutal_cvr / exp_cvr
    })
}
