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

    val a = "qwertyuiop"
    val b = stringHash32(a,79)
    println(a,b)

//      val date = args(0)
//      val hour = args(1)
//
//          val spark = SparkSession.builder()
//            .appName(s"midu_userprofile")
//            .enableHiveSupport()
//            .getOrCreate()
//
//      val sql =
//          s"""
//             |select
//             |  distinct uid
//             |from dl_cpc.cpc_novel_union_events
//             | where day = '$date' and hour = '$hour'
//           """.stripMargin
//
//        println(sql)
//      val allusers = spark.sql(sql)
//      println("alluser:%d".format(allusers.count()))
//
//      val dataDF=allusers
//        .withColumn("hashuid",hash(col("uid")))
//        .withColumn("label",col("hashuid")%500)
//        .filter("label>399")
//        .select("uid")
//        .withColumn("flag", lit(1))
//      println("alluser:%d".format(dataDF.count()))
//      dataDF.show(20)
//
//      val dataRDD2 = allusers.join(dataDF,Seq("uid"),"left")
//        .rdd.map(x => (x.getAs[String](0), x.getAs[Int](1)))

  }

  def hash= udf {
    x:String => {
      var a = stringHash32(x,79)
      if(a<0){
        a += 2^32
      }
       a
    }
  }
}
