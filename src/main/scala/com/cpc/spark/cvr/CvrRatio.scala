package com.cpc.spark.cvr

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import ratio.ratio._

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
        //println(datelist)
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
        println(unionSql)
        val mlFeatureSql =
            s"""
               |select media_appsid,sum(label2) / count(*) * 1000000.0 as acutal_cvr
               |from dl_cpc.ml_cvr_feature_v1
               |where `date` = '$date'
               |and media_appsid not in ('80000001', '80000002', '80001098', '80001292')
               |group by media_appsid
             """.stripMargin
        print(mlFeatureSql)
        val union = spark.sql(unionSql)
        //union.show()
        val mlFeature = spark.sql(mlFeatureSql)
        //mlFeature.show()
        val result = union.join(mlFeature,Seq("media_appsid"),"inner")
          .withColumn("ratio",calRatio(col("exp_cvr"),col("acutal_cvr")))
          .withColumn("date", lit(s"$date"))

        val ratio = result.rdd.map(x => {
            CvRatio(mediaAppsid = x.getAs[String]("media_appsid"),
                ratio = x.getAs[Double]("ratio"),
                date = x.getAs[String]("date"))
        })

//        val ratioListBuffer = scala.collection.mutable.ListBuffer[CvRatio]()
//
//        result.collect().foreach(x => {
//            ratioListBuffer += CvRatio(mediaAppsid = x.getAs[String]("media_appsid"),
//                ratio = x.getAs[Double]("ratio"),
//                date = x.getAs[String]("date"))
//        })

//        val ratioList = ratioListBuffer.toArray
        val ratioList = ratio.collect()

        //将所有的比例小于前90%的调成在90%位置的值
        val ratioD = ratioList.map(x => x.ratio).filter(x => x > 0).sorted
        val th1 = ratioD.length * 0.1
        val ratio1th = ratioD(th1.toInt)

        val ratioList2 = ratioList.map(x => {
            if (x.ratio < ratio1th) {
                x.copy(ratio = ratio1th)
            }
            else if (x.ratio > 1) {
                x.copy(ratio = 1)
            }
            else{
                x
            }
        })

        println("ratioList2 's num is " + ratioList2.length)

        for (r <- ratioList2) {
            println(r.mediaAppsid + " " + r.ratio + " " + r.date)
        }

        val ratioData = Ratio(cvratio = ratioList2)

        ratioData.writeTo(new FileOutputStream("Ratio.pb"))

        println("write to Ratio.pb success!")

//        result.show()
//        //val r = result.collect()
////        println("result 's count is " + r.length)
//        result.coalesce(1)
//          .write.mode("overwrite")
//          .insertInto("dl_cpc.cvrratio")
//        println("insert into dl_cpc.cvrratio success!")
    }
    def calRatio = udf((exp_cvr:Int, acutal_cvr:Int) => {
        1.0 * acutal_cvr / exp_cvr
    })
}
