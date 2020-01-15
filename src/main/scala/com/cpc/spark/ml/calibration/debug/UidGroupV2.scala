package com.cpc.spark.ml.calibration.debug

import com.cpc.spark.common.Murmur3Hash.stringHash32
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * @author WangYao
  * @date 2019/03/14
  */
object UidGroupV2 {
  def main(args: Array[String]): Unit = {

      val date = args(0)
      val label = args(1).toInt

          val spark = SparkSession.builder()
            .appName(s"midu_userprofile")
            .enableHiveSupport()
            .getOrCreate()

      val sql =
          s"""
             |select distinct uid,day
             |from (
             |select if(length(tuid)>0, tuid, uid) as uid, day
             |    from
             |      dl_cpc.cpc_basedata_union_events
             |    where
             |      day = '$date')
           """.stripMargin
    println(sql)
      val data= spark.sql(sql)
        .withColumn("hashuid",hash(label)(col("uid")))
        .withColumn("num",col("hashuid")%1000)
        .withColumn("label",lit(label))
        .select("uid","hashuid","num","day","label")

    data.show(10)

    data.repartition(10).write.mode("overwrite").insertInto("dl_cpc.cvr_mlcpp_uid_label")


  }

  def hash(seed:Int)= udf {
    x:String => {
      var a = stringHash32(x,seed).toDouble
      if(a<0){
        a += scala.math.pow(2,32)
      }
       a
    }
  }
}
