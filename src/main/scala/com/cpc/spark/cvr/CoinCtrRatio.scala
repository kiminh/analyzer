package com.cpc.spark.cvr

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Calendar

import ratio.ratio._
import org.apache.spark.sql.SparkSession

/**
  * @author Jinbao
  * @date 2018/11/23 17:32
  */
object CoinCtrRatio {
    def main(args: Array[String]): Unit = {
        val date = args(0)
        val spark = SparkSession.builder()
          .appName(s"CoinCtrRatio date = $date")
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

        val sql =
            s"""
               |select
               |    a.ideaid as ideaid,
               |    if (exp_ctr > 0, acutal_ctr/exp_ctr , 0) as ratio,
               |    '$date' as date
               |from (
               |    select
               |        ideaid,
               |        sum(isclick) / count(*) as acutal_ctr,
               |        (sum(exp_ctr)/1000000) / count(*) as exp_ctr
               |        from
               |            dl_cpc.cpc_basedata_union_events
               |        where
               |            ($datelist)
               |            and media_appsid in ('80000001','80000002')
               |        group by
               |            ideaid
               |    ) a,
               |    (
               |        select
               |            distinct id as ideaid
               |        from
               |            src_cpc.cpc_idea
               |        where
               |            style_id = 510127
               |    ) b
               |where
               |    a.ideaid = b.ideaid
             """.stripMargin
        println(sql)
        val coinCtrRatio = spark.sql(sql).rdd.map(x =>
            IdeaidRatio(ideaid = x.getAs[Int]("ideaid"),
                ratio = x.getAs[Double]("ratio"),
                date = x.getAs[String]("date"))
        ).collect()
        println("coinCtrRatio 's length is " + coinCtrRatio.length)
        val ratioD = coinCtrRatio.map(x => x.ratio).filter(x => x > 0).sorted
        println("ratioD 's length is " + ratioD.length)

        val th1 = ratioD.length * 0.1
        val ratio1th = ratioD(th1.toInt)

        val ratioList2 = coinCtrRatio.map(x => {
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
            println(r.ideaid + " " + r.ratio + " " + r.date)
        }

        val ratioData = IdeaidCtrRatio(ideaidRatio = ratioList2)

        ratioData.writeTo(new FileOutputStream("IdeaidCtrRatio.pb"))

        println("write to IdeaidCtrRatio.pb success!")
    }
}
