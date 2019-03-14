package com.cpc.spark.novel

import com.alibaba.fastjson.JSON
import com.cpc.spark.streaming.tools.Gzip.decompress
import sys.process._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * @author WangYao
  * @date 2019/03/14
  */
object MiduUserprofile {
    def main(args: Array[String]): Unit = {

        val date = args(0)
        val hour = args(1)
        val spark = SparkSession.builder()
          .appName(s"midu_userprofile")
          .enableHiveSupport()
          .getOrCreate()

        val filename = "/home/cpc/wy/title_adclass.csv"
        val path = s"/user/cpc/wy/title_adclass.csv"
        val movefiletohdfs = s"hadoop fs -put -f ${filename} ${path}"
          movefiletohdfs !
        val title= spark.read.format("csv").load(path).toDF("title","adclass","cate_1","cate_2")
        title.show(5)
        val sql =
            s"""
               |select
               |  imei,title
               |from dl_cpc.cpc_midu_toutiao_log
               |where day='$date' and hour = '$hour'
             """.stripMargin

        println(sql)
      val data2 = spark.sql(sql)
          .join(title,Seq("title"),"left")

      data2.write.mode("overwrite").saveAsTable("test.wy00")
      val youxi=data2
        .filter("cate_2='游戏类'")
        .select("imei")
        .filter("imei is not null").distinct()

      val youxinum= data2.count()
      println("youxi is %d".format(youxinum))

      val meirong=data2
        .filter("cate_1='美容化妆'")
        .select("imei")
        .filter("imei is not null").distinct()

      val meirongnum= data2.count()
      println("meirong is %d".format(meirongnum))

//        data2.repartition(1).write.mode("overwrite").insertInto("dl_cpc.cpc_midu_toutiao_log")
    }
}
