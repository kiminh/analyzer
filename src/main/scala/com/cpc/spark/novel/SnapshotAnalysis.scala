package com.cpc.spark.novel

import com.alibaba.fastjson.JSON
import com.cpc.spark.streaming.tools.Gzip.decompress
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.util.Base64
import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * @author WangYao
  * @date 2019/03/11
  */
object SnapshotAnalysis {
    def main(args: Array[String]): Unit = {
        val date = args(0)
        val hour = args(1)
        val spark = SparkSession.builder()
          .appName(s"Snapshot Analysis date = $date and hour = $hour")
          .enableHiveSupport()
          .getOrCreate()

        val sql =
            s"""
               |select * from dl_cpc.cpc_snapshot where dt = '$date' and hour = '$hour'
             """.stripMargin

        println(sql)
        val data = spark.sql(sql)
          .withColumn("decode_content",decode(col("content")))

        data.show(5)
    }

    def decode = udf {
        (x: String) => {
          if (x != null)  {
             Base64.getDecoder().decode(x)
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
