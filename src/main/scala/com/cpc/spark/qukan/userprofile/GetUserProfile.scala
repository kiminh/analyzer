package com.cpc.spark.qukan.userprofile

import java.util.Calendar
import org.apache.spark.SparkConf

/**
  * Created by Roy on 2017/4/14.
  */
object GetUserProfile {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("CPC get qukan user profile")
    val cal = Calendar.getInstance()
    val hdfs = "/warehouse/rpt_qukan.db/device_member_coin/thedate=%s/*".format(cal.getTime)



  }
}
