package com.cpc.spark.novel

import org.apache.spark.sql.SparkSession
import com.cpc.spark.streaming.tools.Encoding
import org.apache.spark.sql.functions._
import com.cpc.spark.streaming.tools.Gzip.decompress
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}
import com.cpc.spark.novel.CryptBase64Utils
import com.cpc.spark.novel.GnuZip

import com.mysql.jdbc.util.Base64Decoder
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
               |select opt["data"] as data
               |from dl_cpc.cpc_union_trace_log
               |where `date`='2019-03-12' and trace_type ='inform' and opt["chan"] = "midunov"
             """.stripMargin

        println(sql)

      val data = spark.sql(sql)

        data.show(5)
      val data2=data
        .withColumn("data1",decode(col("data")))
        .withColumn("data1",unzip(col("data1")))
        data2.show(1)
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

//    def unzip = udf {
//        (x:String)=>
//        {
//            val inputStream = new GZIPInputStream(this.getClass.getClassLoader.getResourceAsStream(x))
//            val output = scala.io.Source.fromInputStream(inputStream).mkString
//            output
//        }
//    }
}
