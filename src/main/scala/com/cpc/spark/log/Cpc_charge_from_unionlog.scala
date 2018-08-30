package com.cpc.spark.log

import java.sql.ResultSet

import com.cpc.spark.streaming.parser.StreamingDataParserV2
import com.cpc.spark.streaming.tools.MDBManager
import data.Data
import kafka.producer.KeyedMessage
import org.apache.spark.sql.SparkSession


object Cpc_charge_from_unionlog {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    val start = args(0).toLong
    val end = args(1).toLong

    val message = spark.sql(
      s"""
         |SELECT thedate,thehour,field["cpc_click_new"].string_type as value
         |from dl_cpc.src_cpc_click_minute
         |WHERE thedate = "2018-08-30"
         | and thehour in ("17", "18")
         | and log_timestamp>=$start
         | and log_timestamp<=$end
      """.stripMargin)

    val parserData = message.rdd.map { x =>
      val date = x.getAs[String]("thedate")
      val hour = x.getAs[String]("thehour").toInt
      val value = x.getAs[String]("value")
      StreamingDataParserV2.parse_src_click(date, hour, value)
    }

    val base_data = parserData.filter {
      case (isok, adSrc, dspMediaId, dspAdslotId, sid, date, hour, typed, idea_id, unit_id, plan_id, user_id, media_id, adslot_id, adslot_type, price, req, fill, imp, click) =>
        isok
    }.map {
      case (isok, adSrc, dspMediaId, dspAdslotId, sid, date, hour, typed, idea_id, unit_id, plan_id, user_id, media_id, adslot_id, adslot_type, price, req, fill, imp, click) =>
        ((sid, typed, idea_id), (adSrc, dspMediaId, dspAdslotId, date, hour, unit_id, plan_id, user_id, media_id, adslot_id, adslot_type, price, req, fill, imp, click))
    }.reduceByKey {
      case (x, y) => x
    }.map {
      case ((sid, typed, idea_id), (adSrc, dspMediaId, dspAdslotId, date, hour, unit_id, plan_id, user_id, media_id, adslot_id, adslot_type, price, req, fill, imp, click))
      => ((adSrc, dspMediaId, dspAdslotId, date, idea_id, unit_id, plan_id, user_id, media_id, adslot_id, adslot_type), (price, req, fill, imp, click))
    }.reduceByKey {
      case (x, y) =>
        (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5)
    }.map {
      case ((adSrc, dspMediaId, dspAdslotId, date, idea_id, unit_id, plan_id, user_id, media_id, adslot_id, adslot_type), (price, req, fill, imp, click)) =>
        (media_id, adslot_id, adslot_type, idea_id, unit_id, plan_id, user_id, date, req, fill, imp, click, price, adSrc, dspMediaId, dspAdslotId)
    }

    /*base_data.repartition(20).foreachPartition {
      data =>
        //getCurrentDate("start-conn")
        val startTime = System.currentTimeMillis()
        var sendSqlExcuteTime: Double = 0
        var sqlExcuteTime: Double = 0
        var kafakaSendTime: Double = 0
        val conn = MDBManager.getMDBManager(false).getConnection
        val conn2 = MDBManager.getMDB2Manager(false).getConnection
        val dspConn = MDBManager.getDspMDBManager(false).getConnection
        conn.setAutoCommit(false)
        conn2.setAutoCommit(false)
        dspConn.setAutoCommit(false)
        val stmt = conn.createStatement()
        val stmt2 = conn2.createStatement()
        val dspstmt = dspConn.createStatement()
        val data_builder = Data.Log.newBuilder()
        val field_builder = Data.Log.Field.newBuilder()
        val map_builder = Data.Log.Field.Map.newBuilder()
        val valueType_builder = Data.ValueType.newBuilder()
        val ts = System.currentTimeMillis()

        var foreachCount = 0
        var messages = Seq[KeyedMessage[String, Array[Byte]]]()
        data.foreach {
          r =>
            foreachCount = foreachCount + 1
            var sql = ""
            if (r._14 == 1) {
              //`media_id`,`channel_id`,`adslot_id`,`adslot_type`,`idea_id`,`unit_id`,`plan_id`,`user_id`,`date`,`request`,`served_request`,`impression`,`click`,`activation`,`fee`
              sql = "call eval_charge2(" + r._1 + ",0," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6 + "," + r._7 + ",'" + r._8 + "'" + "," + r._9 + "," + r._10 + "," + r._11 + "," + r._12 + ",0," + r._13 + ")"
            } else {
              //`ad_src` 14,`dsp_media_id` 15 ,`dsp_adslot_id` 16, `media_id`1, `channel_id`2 ,`adslot_id`3 , `adslot_type`4 ,`date` , `request` , `served_request`,`impression` , `click`
              sql = "call eval_dsp(" + r._14 + ",'" + r._15 + "','" + r._16 + "'," + r._1 + ",0," + r._2 + "," + r._3 + ",'" + r._8 + "'" + "," + r._9 + "," + r._10 + "," + r._11 + "," + r._12 + ")"
              println("call eval_dsp:" + sql)
            }
            var reset: ResultSet = null
            val sendSqlStartTime = System.currentTimeMillis()
            if (r._14 == 1) {
              if (r._6 >= 1000000 || r._6 == 0) {
                reset = stmt2.executeQuery(sql)
              } else {
                reset = stmt.executeQuery(sql)
              }
            } else {
              reset = dspstmt.executeQuery(sql)
            }
            var store_sql_status = 2
            var consumeCoupon = 0
            var consumeBalance = 0
            var executeTime: Double = 0
            while (reset.next()) {
              try {
                store_sql_status = reset.getInt(1)
                consumeBalance = reset.getInt(2)
                consumeCoupon = reset.getInt(3)
                executeTime = reset.getDouble(4)
                sqlExcuteTime = sqlExcuteTime + executeTime
              } catch {
                case ex: Exception =>
                  ex.printStackTrace()
                  store_sql_status = 3
              }
            }
            if (store_sql_status != 0) {
              println("update sql error:" + sql)
            }
            sendSqlExcuteTime += System.currentTimeMillis() - sendSqlStartTime

        }
        conn.commit()
        conn.close()
        conn2.commit()
        conn2.close()
        dspConn.commit()
        dspConn.close()
    }*/

    //测试
    base_data.collect().foreach {
      r =>
        var sql = ""
        if (r._14 == 1) {
          //`media_id`,`channel_id`,`adslot_id`,`adslot_type`,`idea_id`,`unit_id`,`plan_id`,`user_id`,`date`,`request`,`served_request`,`impression`,`click`,`activation`,`fee`
          sql = "call eval_charge2(" + r._1 + ",0," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6 + "," + r._7 + ",'" + r._8 + "'" + "," + r._9 + "," + r._10 + "," + r._11 + "," + r._12 + ",0," + r._13 + ")"
        } else {
          //`ad_src` 14,`dsp_media_id` 15 ,`dsp_adslot_id` 16, `media_id`1, `channel_id`2 ,`adslot_id`3 , `adslot_type`4 ,`date` , `request` , `served_request`,`impression` , `click`
          sql = "call eval_dsp(" + r._14 + ",'" + r._15 + "','" + r._16 + "'," + r._1 + ",0," + r._2 + "," + r._3 + ",'" + r._8 + "'" + "," + r._9 + "," + r._10 + "," + r._11 + "," + r._12 + ")"
        }
        println(sql)
    }
  }
}