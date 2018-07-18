package com.cpc.spark.streaming.anal

import com.cpc.spark.common.LogData
import com.cpc.spark.common.Ui
import com.cpc.spark.common.Event
import com.cpc.spark.streaming.tools.Utils
import java.util.Calendar
import java.text.SimpleDateFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import kafka.producer.KeyedMessage
import java.sql.ResultSet
import com.cpc.spark.streaming.tools.MDBManager
import data.Data
import scala.collection.Iterator
object RecoveryStreamingData {

  def main(args: Array[String]): Unit = {
    if (args.length < 6) {
      System.err.println(
        s"""
           |Usage: RecoveryStreamingData <brokers> <out_topics>  <date> <hour> <startTimestamp> <endTimestamp>
           |
        """.stripMargin)
      System.exit(1)
    }
    val brokers = args(0)
    val out_topics = args(1)
    val date1 = args(2)
    val hour = args(3)
    val startTimestamp = args(4).toInt
    val endTimestamp = args(5).toInt
    println("date:" + date1)
    println("hour:" + hour)

    val ctx = SparkSession.builder()
      .appName("RecoveryStreamingData from %s/%s".format(date1, hour))
      .enableHiveSupport()
      .getOrCreate()
      
    import ctx.implicits._
    var sql = "";
    if(startTimestamp > 0  && endTimestamp > 0){
        sql = s"""SELECT *
           FROM dl_cpc.cpc_union_log where  `date` = "%s" and hour = "%s" and timestamp > %s and timestamp < %s
       """.stripMargin.format(date1, hour, startTimestamp, endTimestamp)
    }else if(startTimestamp > 0 ){
       sql = s"""SELECT *
           FROM dl_cpc.cpc_union_log where  `date` = "%s" and hour = "%s" and timestamp > %s 
       """.stripMargin.format(date1, hour,  startTimestamp)
    }else if(endTimestamp > 0 ){
        sql = s"""SELECT *
           FROM dl_cpc.cpc_union_log where  `date` = "%s" and hour = "%s" and timestamp < %s
       """.stripMargin.format(date1, hour,  endTimestamp)
    }else{
      sql = s"""SELECT *
           FROM dl_cpc.cpc_union_log where  `date` = "%s" and hour = "%s"
       """.stripMargin.format(date1, hour)
    }
    println("sql:"+ sql)
    val rddLog = ctx.sql(sql)
      .as[UnionLog]
      .rdd.cache()

    val base_data = rddLog.map {
      case union =>
        //(date, idea_id, unit_id, plan_id, user_id, media_id, adslot_id, adslot_type), (price, req, fill, imp, click))
        ((union.date, union.ideaid, union.unitid, union.planid, union.userid, union.media_appsid, union.adslotid, union.adslot_type), (union.price, 1, union.isfill, union.isshow, union.isclick))
    }.map{
      case ((date, idea_id, unit_id, plan_id, user_id, media_id, adslot_id, adslot_type), (price, req, fill, imp, click)) =>
        {
          var realPrice = 0
          if(click == 1){
            realPrice = price
          }
          ((date, idea_id, unit_id, plan_id, user_id, media_id, adslot_id, adslot_type), (realPrice, req, fill, imp, click))
        }
    }.reduceByKey {
      case (x, y) =>
        (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5)
    }.map {
      case ((date, idea_id, unit_id, plan_id, user_id, media_id, adslot_id, adslot_type), (price, req, fill, imp, click)) =>
        (media_id, adslot_id, adslot_type, idea_id, unit_id, plan_id, user_id, date, req, fill, imp, click, price)
    }
    println("****************************************")
    val destData = base_data.collect()
    destData.foreach(println)
    var req = 0;
    var fill = 0;
    var imp = 0;
    var click = 0;
    var price = 0;
   destData.foreach(x =>{
          req += x._9
    		  fill += x._10
    		  imp += x._11
    		  click += x._12
    		  price += x._13
      
    })
    println("****************************************")
    println("req: " + req +" fill: " + fill+" imp: " + imp+" click: " + click+" price: " + price)
    //(partition:Iterator[(Int, Int, Int, Int, Int, Int, Int, String, Int, Int, Int, Int, Int)])
          val conn = MDBManager.getMDBManager(false).getConnection
          val conn2 = MDBManager.getMDB2Manager(false).getConnection
          conn.setAutoCommit(false)
          conn2.setAutoCommit(false)
          val stmt = conn.createStatement()
          val stmt2 = conn2.createStatement()
          val data_builder = Data.Log.newBuilder()
          val field_builder = Data.Log.Field.newBuilder()
          val map_builder = Data.Log.Field.Map.newBuilder()
          val valueType_builder = Data.ValueType.newBuilder()
          val ts = System.currentTimeMillis();
          var producer = com.cpc.spark.streaming.tools.KafkaUtils.getProducer(brokers)
    destData.foreach(r  => {         
                //`media_id`,`channel_id`,`adslot_id`,`adslot_type`,`idea_id`,`unit_id`,`plan_id`,`user_id`,`date`,`request`,`served_request`,`impression`,`click`,`activation`,`fee`
              val sql = "call eval_charge2(" + r._1 + ",0," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6 + "," + r._7 + ",'" + r._8 + "'" + "," + r._9 + "," + r._10 + "," + r._11 + "," + r._12 + ",0," + r._13 + ")"
              //              println("sql = " + sql)
              println("call-sql:" + sql)
              var reset:ResultSet = null
              if(r._6 >= 1000000 || r._6 == 0){
                 reset = stmt2.executeQuery(sql)
              }else{
                 reset = stmt.executeQuery(sql)
              }
              
              
              var store_sql_status = 2
              var consumeCoupon = 0
              var consumeBalance = 0
              while (reset.next()) {
                try {
                  store_sql_status = reset.getInt(1)
                  consumeBalance = reset.getInt(2)
                  consumeCoupon = reset.getInt(3)
                } catch {
                  case ex: Exception =>
                    ex.printStackTrace()
                    store_sql_status = 3
                }
              }
              println("store_sql_status:" + store_sql_status)
              println("consumeBalance:" + consumeBalance)
              println("consumeCoupon:" + consumeCoupon)
              data_builder.clear()
              field_builder.clear()
              map_builder.clear()
              valueType_builder.clear()
              //media_id, adslot_id, adslot_type, idea_id, unit_id, plan_id, user_id, date, req, fill, imp, click, price
              data_builder.setLogTimestamp(ts)

              for (i <- 0 to r.productArity - 1) {
                valueType_builder.clear()
                map_builder.clear()
                if (i == 7) {
                  valueType_builder.setStringType(r.productElement(i).toString())
                } else {
                  valueType_builder.setIntType(r.productElement(i).toString().toInt)
                }
                val valueType = valueType_builder.build()
                val key = switchKeyById(i)
                map_builder.setKey(key)
                map_builder.setValue(valueType)
                field_builder.addMap(map_builder.build())
              }

              valueType_builder.clear()
              map_builder.clear()
              valueType_builder.setIntType(0)
              map_builder.setKey("channel_id")
              map_builder.setValue(valueType_builder.build())
              map_builder.setKey("activation")
              map_builder.setValue(valueType_builder.build())
              field_builder.addMap(map_builder.build())

              valueType_builder.clear()
              map_builder.clear()
              valueType_builder.setIntType(store_sql_status)
              map_builder.setKey("sql_status")
              map_builder.setValue(valueType_builder.build())
              field_builder.addMap(map_builder.build())

              valueType_builder.clear()
              map_builder.clear()
              valueType_builder.setIntType(consumeBalance)
              map_builder.setKey("balance")
              map_builder.setValue(valueType_builder.build())
              field_builder.addMap(map_builder.build())

              valueType_builder.clear()
              map_builder.clear()
              valueType_builder.setIntType(consumeCoupon)
              map_builder.setKey("coupon")
              map_builder.setValue(valueType_builder.build())
              field_builder.addMap(map_builder.build())
              
              valueType_builder.clear()
              map_builder.clear()
              valueType_builder.setStringType(sql)
              map_builder.setKey("sql")
              map_builder.setValue(valueType_builder.build())
              field_builder.addMap(map_builder.build())

              valueType_builder.clear()
              valueType_builder.setStringType(r._1 + ",0," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6 + "," + r._7 + ",'" + r._8 + "'" + "," + r._9 + "," + r._10 + "," + r._11 + "," + r._12 + ",0," + r._13 + "," + consumeBalance + "," + consumeCoupon)
              map_builder.setKey("line")
              map_builder.setValue(valueType_builder.build())
              field_builder.addMap(map_builder.build())
              
              data_builder.setField(field_builder.build())
              val message = new KeyedMessage[String, Array[Byte]](out_topics, null, data_builder.build().toByteArray())
              try {
                producer.send(message)
              } catch {
                case ex: Exception =>
                  ex.printStackTrace()
                  if (producer != null) {
                    producer.close()
                    producer = null
                  }
                  producer = com.cpc.spark.streaming.tools.KafkaUtils.getProducer(brokers)
              }
    })
    conn.commit()
    conn.close()
    conn2.commit()
    conn2.close()
    producer.close()
    ctx.stop();
  }
  
 def switchKeyById(idx: Int): String = {
    var res: String = "None"
    //media_id, adslot_id, adslot_type, idea_id, unit_id, plan_id, user_id, date, req, fill, imp, click, price
    idx match {
      case 0  => res = "media_id";
      case 1  => res = "adslot_id";
      case 2  => res = "adslot_type";
      case 3  => res = "idea_id";
      case 4  => res = "unit_id";
      case 5  => res = "plan_id";
      case 6  => res = "user_id";
      case 7  => res = "date";
      case 8  => res = "req";
      case 9  => res = "fill";
      case 10 => res = "imp";
      case 11 => res = "click";
      case 12 => res = "price";
    }
    res
  }
}

case class UnionLog(
    searchid: String = "",
    timestamp: Int = 0,
    network: Int = 0,
    ip: String = "",
    exptags: String = "",
    media_type: Int = 0,
    media_appsid: String = "0",
    adslotid: String = "0",
    adslot_type: Int = 0,
    adnum: Int = 0,
    isfill: Int = 0,
    adtype: Int = 0,
    adsrc: Int = 0,
    interaction: Int = 0,
    bid: Int = 0,
    floorbid: Float = 0,
    cpmbid: Float = 0,
    price: Int = 0,
    ctr: Long = 0,
    cpm: Long = 0,
    ideaid: Int = 0,
    unitid: Int = 0,
    planid: Int = 0,
    country: Int = 0,
    province: Int = 0,
    city: Int = 0,
    isp: Int = 0,
    brand: String = "",
    model: String = "",
    uid: String = "",
    ua: String = "",
    os: Int = 0,
    screen_w: Int = 0,
    screen_h: Int = 0,
    sex: Int = 0,
    age: Int = 0,
    coin: Int = 0,
    isshow: Int = 0,
    show_timestamp: Int = 0,
    show_network: Int = 0,
    show_ip: String = "",
    isclick: Int = 0,
    click_timestamp: Int = 0,
    click_network: Int = 0,
    click_ip: String = "",
    antispam_score: Int = 0,
    antispam_rules: String = "",
    duration: Int = 0,
    userid: Int = 0,
    interests: String = "",
    ext: collection.Map[String, ExtValue] = null,
    date: String = "",
    hour: String = "")
  case class ExtValue(int_value: Int = 0, long_value: Long = 0, float_value: Float = 0, string_value: String = "")