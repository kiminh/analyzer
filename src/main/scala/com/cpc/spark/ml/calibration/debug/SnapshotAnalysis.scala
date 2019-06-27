package com.cpc.spark.ml.calibration.debug

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.util.Base64
import com.google.protobuf.CodedInputStream
import scala.collection.JavaConversions._
import mlmodel.mlmodel.{Feature, FeatureStore}

/**
  * @author WangYao
  * @date 2019/03/11
  */
object SnapshotAnalysis {
    def main(args: Array[String]): Unit = {
      val date = args(0)
      val hour = args(1)
      val spark = SparkSession.builder()
          .appName(s"Snapshot Analysis date = $date and hour = '$hour'")
          .enableHiveSupport()
          .getOrCreate()
      import spark.implicits._

        val sql =
            s"""
               |select * from dl_cpc.cpc_snapshot where dt = '$date' and hour = '$hour' and pt = 'qtt-cvr'
             """.stripMargin

        println(sql)
        val data = spark.sql(sql)
          .withColumn("decode_content",decode(col("content")))
          .rdd.map(r=>{
          val searchid = r.getAs[String]("searchid")
          val dt = r.getAs[String]("dt")
          val hour = r.getAs[String]("hour")
          val ideaid = r.getAs[Long]("ideaid")
          val adslotid = r.getAs[String]("adslotid")
          val content = r.getAs[Array[Byte]]("decode_content")
          val contentvalue = new FeatureStore().mergeFrom(CodedInputStream.newInstance(content)).features
          var key = ""
          var md5 = ""
          var user_req_ad_num = ""
          var i = 0
          var raw_cvr = 0
          var postcali_cvr = 0
          var exp_cvr = 0
          var model = ""
          var adclass = ""
          var unitid = ""
          while (i < contentvalue.size){
            val name = contentvalue(i).name
            if (name == "calibrations_key")
              {
                key = contentvalue(i).strList.get(0)
              }
            else if (name == "calibrations_md5")
            {
              md5 = contentvalue(i).strList.get(0)
            }
            else if (name == "snapshot_postcali_vaule")
            {
             postcali_cvr = contentvalue(i).intList.get(0)
            }
            else if (name == "snapshot_expvalue")
            {
              exp_cvr = contentvalue(i).intList.get(0)
            }
            else if (name == "snapshot_user_req_ad_num")
            {
              user_req_ad_num = contentvalue(i).strList.mkString("")
            }
            else if (name == "snapshot_unitid")
            {
              unitid = contentvalue(i).strList.mkString("")
            }
            else if (name == "model_name")
            {
              model = contentvalue(i).strList.get(0)
            }
            else if (name == "snapshot_raw_expvalue")
            {
              raw_cvr = contentvalue(i).intList.get(0)
            }
            else if (name == "snapshot_adclass")
            {
              adclass = contentvalue(i).strList.mkString("")
            }
            i += 1
          }
          (searchid,postcali_cvr,model,dt,hour)
        }).toDF("searchid","postcali_cvr","model","dt","hour")
         .filter("model in ('qtt-cvr-dnn-rawid-v1-180','qtt-cvr-dnn-rawid-v1-180-newcali')")

        data.show(10)
      data.repartition(100).write.mode("overwrite").insertInto("dl_cpc.snapshot_analysis")
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
