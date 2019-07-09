package com.cpc.spark.ml.calibration.debug

import com.cpc.spark.qukan.userprofile.SetUserProfileTag.SetUserProfileTagInHiveHourly
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import com.cpc.spark.common.Murmur3Hash.stringHash32

/**
  * @author WangYao
  * @date 2019/03/14
  */
object UidGroup {
  def main(args: Array[String]): Unit = {

      val date = args(0)

          val spark = SparkSession.builder()
            .appName(s"midu_userprofile")
            .enableHiveSupport()
            .getOrCreate()

      val sql =
          s"""
             |select distinct uid, from_unixtime(unix_timestamp(day,'yyyy-mm-dd'),'yyyymmdd') as dt
             |    from
             |      dl_cpc.cpc_basedata_union_events
             |    where
             |      day = '$date'
             |      and media_appsid in ('80000001', '80000002')
           """.stripMargin
    println(sql)
      val data= spark.sql(sql)
        .withColumn("hashuid",hash(concat(col("uid"),col("dt"))))
        .withColumn("num",col("hashuid")%1000)
        .withColumn("label",when(col("num")>950,lit(1)).otherwise(lit(0)))
        .select("uid","hashuid","num","label","dt")

    data.show(10)

    data.repartition(10).write.mode("overwrite").insertInto("dl_cpc.cvr_mlcpp_uid_label")


  }

  def hash= udf {
    x:String => {
      var a = stringHash32(x,79).toDouble
      if(a<0){
        a += scala.math.pow(2,32)
      }
       a
    }
  }
}
