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

    val b = "qwertyuiop"
    var a = stringHash32(b,79).toDouble
    if(a<0){
      a += scala.math.pow(2,32)
    }
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
