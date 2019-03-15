package com.cpc.spark.novel

import com.alibaba.fastjson.JSON
import com.cpc.spark.streaming.tools.Gzip.decompress
import sys.process._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.sql.functions._
import java.net.URL
import java.io.File
import scala.collection.JavaConversions._
import scala.collection.mutable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

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
        import spark.implicits._

        val filename = "/home/cpc/wy/title_adclass.txt"
        val path = s"/user/cpc/wy/title_adclass.txt"
        val movefiletohdfs = s"hadoop fs -put -f ${filename} ${path}"
          movefiletohdfs !
        val title= spark.sparkContext.textFile(path)
          .map(x=>x.split(",")).map(x=> Row(x(0),x(1).toInt,x(2),x(3)))

        val schema: StructType = (new StructType)
          .add("title", StringType)
          .add("adclass", IntegerType)
          .add("cate_1", StringType)
          .add("cate_2", StringType)
        //根据rdd和schema信息创建DataFrame
        val titleDF: DataFrame = spark.createDataFrame(title, schema)
          titleDF.show(5)
        val sql =
            s"""
               |select
               |  imei,title
               |from dl_cpc.cpc_midu_toutiao_log
               |where day='$date' and hour = '$hour'
             """.stripMargin

        println(sql)
      val data2 = spark.sql(sql)
          .join(titleDF,Seq("title"),"left")

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
