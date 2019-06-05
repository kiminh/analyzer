package com.cpc.spark.novel

import java.io.FileInputStream

import com.alibaba.fastjson.JSON
import com.cpc.spark.streaming.tools.Gzip.decompress
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.util.Base64

import com.google.protobuf.CodedInputStream

import scala.collection.JavaConversions._
import scala.collection.mutable
import mlmodel.mlmodel.{Feature, FeatureStore}

/**
  * @author WangYao
  * @date 2019/03/11
  */
object SnapshotAnalysis {
    def main(args: Array[String]): Unit = {
      import spark.implicits._
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
          .rdd.map(r=>{
          val searchid = r.getAs[String]("searchid")
          val content = r.getAs[Array[Byte]]("decode_content")
          val contentvalue = new FeatureStore().mergeFrom(CodedInputStream.newInstance(content)).features
          var key = ""
          var md5 = ""
          var postcali_value = 0
          for (i <- contentvalue.size){
            val name = contentvalue(i).name
            if (name == "calibrations_key")
              {
                key = contentvalue(i).strList.mkString("")
              }
            if (name == "calibrations_md5")
            {
              md5 = contentvalue(i).strList.mkString("")
            }
            if (name == "calibrations_md5")
            {
             postcali_value = contentvalue(i).intList.get(0)
            }

          }
          (searchid,postcali_value,key,md5)
        }).toDF("searchid","postcali_value","key","md5")

        data.show(10)
      data.write.mode("overwrite").saveAsTable("test.wy00")
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
}
