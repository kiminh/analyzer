

package com.cpc.spark.ocpc

import java.text.SimpleDateFormat
import java.util.Calendar

import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import userprofile.Userprofile.UserProfile

import scala.collection.mutable.ListBuffer
import userocpc.userocpc._
import java.io.FileOutputStream




object OcpcTestSamplePb {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // calculate time period for historical data
    val end_date = args(0)
    val hour = args(1)
    val threshold = 20
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val date = sdf.parse(end_date)
    val calendar = Calendar.getInstance
    calendar.setTime(date)
    calendar.add(Calendar.DATE, -7)
    val dt = calendar.getTime
    val start_date = sdf.format(dt)
    val selectCondition1 = s"`date`='$start_date' and hour > '$hour'"
    val selectCondition2 = s"`date`>'$start_date' and `date`<'$end_date'"
    val selectCondition3 = s"`date`='$end_date' and hour <= '$hour'"

    // read data and set redis configuration
    val sqlRequest =
      s"""
         |SELECT
         |  ideaid,
         |  adclass,
         |  date,
         |  hour,
         |  SUM(cost) as cost,
         |  SUM(ctr_cnt) as ctr_cnt,
         |  SUM(cvr_cnt) as cvr_cnt,
         |  SUM(total_cnt) as total_cnt
         |FROM
         |  dl_cpc.ocpc_uid_userid_track_label2
         |WHERE ($selectCondition1) OR
         |($selectCondition2) OR
         |($selectCondition3)
         |GROUP BY ideaid, adclass, date, hour
       """.stripMargin
    println(sqlRequest)

    val base = spark.sql(sqlRequest)

    base.show(10)

  }


  def savePbPack(dataset: Dataset[Row]): Unit = {
    var list = new ListBuffer[SingleUser]
    val filename = s"UseridDataOcpc.pb"
    println("size of the dataframe")
    println(dataset.count)
    dataset.show(10)
    for (record <- dataset.collect()) {

      val ideaid = record.get(0).toString
      val userId = record.get(1).toString
      val adclassId = record.get(2).toString
      val costValue = record.get(3).toString
      val ctrValue = record.getLong(4).toString
      val cvrValue = record.getLong(5).toString
      val adclassCost = record.get(6).toString
      val adclassCtr = record.getLong(7).toString
      val adclassCvr = record.getLong(8).toString
      val k = record.get(9).toString

      val currentItem = SingleUser(
        ideaid = ideaid,
        userid = userId,
        cost = costValue,
        ctrcnt = ctrValue,
        cvrcnt = cvrValue,
        adclass = adclassId,
        adclassCost = adclassCost,
        adclassCtrcnt = adclassCtr,
        adclassCvrcnt = adclassCvr,
        kvalue = k
      )
      list += currentItem
    }
    val result = list.toArray[SingleUser]
    val useridData = UserOcpc(
      user = result
    )
    println("length of the array")
    println(result.length)
    useridData.writeTo(new FileOutputStream(filename))
    println("complete save data into protobuffer")

  }
}
