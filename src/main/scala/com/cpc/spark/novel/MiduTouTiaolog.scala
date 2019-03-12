package com.cpc.spark.novel

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.cpc.spark.streaming.tools.Gzip.decompress
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * @author WangYao
  * @date 2019/03/11
  */
object MiduTouTiaolog {
    def main(args: Array[String]): Unit = {
        val date = args(0)
        val hour = args(1)
        val spark = SparkSession.builder()
          .appName(s"midu_toutiao date = $date and hour = $hour")
          .enableHiveSupport()
          .getOrCreate()

        val sql =
            s"""
               |select
               |  searchid, opt["data"] as opt,day,hour
               |from dl_cpc.cpc_basedata_trace_event
               |where day='2019-03-12' and hour = '10' and trace_type ='inform' and opt["chan"] = "midunov"
             """.stripMargin

        println(sql)
      val data = spark.sql(sql)
        .withColumn("opt",decode(col("opt")))
        .withColumn("opt",unzip(col("opt")))
        .withColumn("opt_map",strToMap(col("opt")))

      data.createOrReplaceTempView("tmp")
      val sql2 =
        s"""
           |select
           |  searchid, opt_map,opt_map["appscore"] as appscore,
           |  opt_map["ButtonText"] as ButtonText,
           |  opt_map["CommentNum"] as CommentNum,
           |  opt_map["ImageMode"] as ImageMode,
           |  opt_map["InteractionType"] as InteractionType,
           |  opt_map["Source"] as Source,
           |  opt_map["Title"] as Title,
           |  opt_map["imageList"] as imageList
           |  from tmp
             """.stripMargin
      val data2 = spark.sql(sql2)
        data2.show(5)
        data2.write.mode("overwrite").saveAsTable("test.wy03")
//        spark.sql(sql).write.mode("overwrite").insertInto("dl_cpc.cpc_midu_toutiao_log")

    }

    def decode = udf {
        (x: Array[Byte]) => {
          if (x != null)  {
            CryptBase64Utils.cryptBase64ToByteArray(x)
          }
          else
            null
        }
    }

    def unzip = udf {
        (x:Array[Byte]) =>
            if (x != null) decompress(x) else null
    }


    def strToMap= udf{
        (str: String)=>{

          val json = JSON.parseObject(str)
          var map = mutable.LinkedHashMap[String,String]()

          for (k <- json.keySet()) {
            map += (k -> json.getString(k))
          }
          map
        }
    }
}
