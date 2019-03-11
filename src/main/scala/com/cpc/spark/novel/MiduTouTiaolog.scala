package com.cpc.spark.novel

import org.apache.spark.sql.SparkSession
import com.cpc.spark.streaming.tools.Encoding
import org.apache.spark.sql.functions._
import com.cpc.spark.streaming.tools.Gzip.decompress
import java.io.{ByteArrayOutputStream, ByteArrayInputStream}
import java.util.zip.{GZIPOutputStream, GZIPInputStream}
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

//        val sql =
//            s"""
//               |select opt["data"] as data
//               |from dl_cpc.cpc_union_trace_log
//               |where `date`='2019-03-10' and trace_type ='inform' and opt["chan"] = "midunov"
//             """.stripMargin
//
//        println(sql)
            val sql =
                    s"""
                       |select 'E8xHlllllllllWaJBeYWRTBM/eYa_4+z3GR-E30TQnDinLc5FXV9rtd1STET-rDwRZdusqupZHH1aGDRMLbre7PG/VkITEqQIJb79KNagzHlBZG4fABPr1+QxvYTO_DJA3_NBmpZ181trAO4W8WkErQdC_rwlqVTcRENc1LVlN_bpK4Pbq5O-RTywZO9DTWIFN+l4ykgs95rQk8JbrTpkOrgim1Gkxgce7u-Zz1HwVQf/RV9sABHxwH99i8/2xFV1JIz9/Q2nNRXg-LDmqFB9_LEFCZvXkyr3-qyD7q/ETPb+/s07OF/hzOU2/dZXB52G8pZZSdBbKwmbhyPpMWAA5flzQiB_VeX_Ha1e9K7udWQcz_gIehBkCRGldByY09SJpi/-K4_RedVAsFM5OXG1Ii7oskIxFNplRll'
                       | as data
                       |from test.wy00 limit 1
                     """.stripMargin

      val data1 = spark.sql(sql)
        data1.write.mode("overwrite").saveAsTable("test.wy01")
        data1.show(1)

        val data2 =data1.withColumn("data1",decode(col("data")))
        data2.show(1)
        data2.printSchema()
       data2.write.mode("overwrite").saveAsTable("test.wy02")

        val data3 =data2.withColumn("data2",unzip(col("data1")))
        data3.show(1)
        data3.write.mode("overwrite").saveAsTable("test.wy03")
//        spark.sql(sql).write.mode("overwrite").insertInto("dl_cpc.cpc_midu_toutiao_log")

    }

    def decode = udf {
        (x: String) => if (x != null) Encoding.base64Decoder(x) else null
    }

    def unzip1 = udf {
        (x:Array[Byte]) =>
            if (x != null) decompress(x) else null
    }

    def unzip = udf {
        (x:Array[Byte])=>
        {
            val inputStream = new GZIPInputStream(new ByteArrayInputStream(x))
            val output = scala.io.Source.fromInputStream(inputStream).mkString
            output
        }
    }

    def unzip2 = udf {
        (x:String)=>
        {
            val s = new String(x);
            val inputStream = new GZIPInputStream(this.getClass.getClassLoader.getResourceAsStream(s))
            val output = scala.io.Source.fromInputStream(inputStream).mkString
            output
        }
    }
}
