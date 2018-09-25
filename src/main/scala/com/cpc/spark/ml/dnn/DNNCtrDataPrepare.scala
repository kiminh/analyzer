package com.cpc.spark.ml.dnn


import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


/**
  * 获取dnn ctr训练的数据，生成tfrecord格式保存到 /user/dnn/ctr/traindata下
  * created time : 2018/9/25 15:15
  *
  * @author zhj
  * @version 1.0
  *
  */
object DNNCtrDataPrepare {
  def main(args: Array[String]): Unit = {

    val date = "2018-09-24" //args(0)

    val last_3_day = getDays(date, 3, 1).head

    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    //获取app数据
    val app_data = spark.sql(
      s"""
         |select uid,pkgs
         |from dl_cpc.cpc_user_installed_apps
         |where load_date='$date'
      """.stripMargin)

    /*val writer = new PrintWriter(new File("/home/cpc/zhj/app_map/map.txt"))
    val app_map = apps.select(explode($"pkgs").alias("pkg")).groupBy("pkg").count()
      .sort($"count".desc)
      .take(5000).zipWithIndex
      .map {
        x =>
          writer.write(x._1.getAs[String]("pkg") + "," + x._2 + "\n")
          (x._1.getAs[String]("pkg"), x._2)
      }.toMap
    writer.close()


    val a_p = spark.sparkContext.broadcast(app_map)

    val getAppVec = udf {
      pkgs: Seq[String] =>
        val m = a_p.value
        pkgs.map(pkg => m.getOrElse(pkg, -1)).filter(_ >= 0)
    }*/
    //获取广告点击数据
    spark.sql(
      s"""
         |select uid,collect_set(ideaid) as ideaids
         |from dl_cpc.ml_cvr_feature_v1
         |where date > ${getDay(date, 3)}
         |  and date <= $date
         |  and ideaid > 0
         |  and adslot_type = 1
         |  and media_appsid in ('80000001','80000002')
      """.stripMargin)

    //
    val data = spark.sql(
      s"""
         |select uid,hour,sex,age,os,network,city,
         |      media_appsid,adslotid,phone_level,adclass,
         |      adtype,adslot_type,planid,unitid,ideaid,
         |      if(label>0, array(1,0), array(0,1)) as label
         |from dl_cpc.ml_cvr_feature_v1
         |where date = '$date'
         |  and ideaid > 0
         |  and adslot_type = 1
         |  and media_appsid in ('80000001','80000002')
      """.stripMargin)
      .persist()

    /*//获取广告点击数据
    data.where("label = array(1,0)")
      .groupBy($"uid").agg(collect_set($"ideaid").alias("ideaids"))
      .coalesce(1)
      .write.mode("overwrite")
      .parquet(s"/home/cpc/zhj/ctr/dnn/data/click_data/$date")

    val ad_data = spark.read
      .parquet("/home/cpc/zhj/ctr/dnn/data/click_data/{" + getDays(date, 0, 3).mkString(",") + "}")
      .select($"uid", explode($"ideaids").alias("ideaid"))
      .groupBy("uid")
      .agg(collect_set("ideaid").alias("ideaids"))
*/
    //合并数据
    val



    //获取hash code
    val hash = udf {
      num: Int => num.hashCode()
    }

  }

  //获取时间序列
  def getDays(startdate: String, day1: Int = 0, day2: Int): Seq[String] = {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    cal.setTime(format.parse(startdate))
    cal.add(Calendar.DATE, -day1)
    var re = Seq(format.format(cal.getTime))
    for (i <- 1 until day2) {
      cal.add(Calendar.DATE, -1)
      re = re :+ format.format(cal.getTime)
    }
    re
  }

  //获取时间
  def getDay(startdate: String, day: Int): String = {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    cal.setTime(format.parse(startdate))
    cal.add(Calendar.DATE, -day)
    format.format(cal.getTime)
  }

}
