package com.cpc.spark.antispam.anal

import com.cpc.spark.log.parser.{ExtValue, UnionLog}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by wanli on 2017/8/4.
  */
object GetDeviceAnal {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: day <date>
        """.stripMargin)
      System.exit(1)
    }
    val date1 = args(0)
    val date2 = args(1)
    Logger.getRootLogger.setLevel(Level.WARN)
    Logger.getRootLogger.setLevel(Level.WARN)
    val ctx = SparkSession.builder()
      .appName("device anal")
      .enableHiveSupport()
      .getOrCreate()
    import ctx.implicits._
    var sql1 = "SELECT * from  dl_cpc.cpc_union_log where `date` ='%s'  and ext['device_ids'].string_value != '' ".format(date1)
    var sql2 = "SELECT * from  dl_cpc.cpc_union_log where `date` ='%s'  and ext['device_ids'].string_value != '' ".format(date2)

    println("sql1:"+ sql1)
    println("sql2:"+ sql2)
    var unionRdd = ctx.sql(sql1).as[UnionLog].rdd.map(
      x => (x.media_appsid,x.adslotid,x.adslot_type, x.model, x.brand,x.os , x.ext.getOrElse("device_ids",ExtValue()).string_value,x.city)
    ).cache()
   var unionAll =  unionRdd.map{
      case (media_appsid, adslotid, adslot_type, model, brand, os, device_ids, city) =>
        ((media_appsid, adslotid, adslot_type), 1)
    }.reduceByKey((x, y) => x+y)
    var union1 = unionRdd
      .filter(x => x._7.length >0)
      .map{
        case (media_appsid, adslotid, adslot_type, model, brand, os, device_ids, city) =>
        /*  OS_UNKNOWN = 0;
          OS_ANDROID = 1;
          OS_IOS		= 2;
          OS_WP 		= 3;*/
          var deviceIdsArr = device_ids.split(";")
          var imei = ""
          var androidId = ""
          var idfa = ""
          deviceIdsArr.foreach{
            x =>
              var arr = x.split(":")
              if(arr.length == 2){
                if(arr(0) == "DEVID_IMEI"){
                  imei = arr(1)
                }else if(arr(0) == "DEVID_ANDROIDID"){
                  androidId = arr(1)
                }else if(arr(0) == "DEVID_IDFA"){
                  idfa = arr(1)
                }
              }

          }
          (media_appsid, adslotid, adslot_type, model, brand,os , imei, androidId,idfa, city)
      }.filter{
      case (media_appsid, adslotid, adslot_type, model, brand,os , imei, androidId, idfa, city) =>
        if(os == 1 && imei.length > 0 ){
          true
        /*}else if (os == 2 && idfa.length >0){
           true*/
        }else{
           false
        }
    }.map{
      case (media_appsid, adslotid, adslot_type, model, brand,os , imei, androidId,idfa, city) =>
        ((media_appsid, adslotid, adslot_type, imei),(model, brand, os, imei, androidId, idfa, city))
    }.reduceByKey((x, y) => x)
    var union2 =  ctx.sql(sql2).as[UnionLog].rdd.map(
      x => (x.media_appsid,x.adslotid,x.adslot_type, x.model, x.brand,x.os , x.ext.getOrElse("device_ids",ExtValue()).string_value,x.city)
    )
      .filter(x => x._7.length >0)
      .map{
        case (media_appsid, adslotid, adslot_type, model, brand, os, device_ids, city) =>
          /*  OS_UNKNOWN = 0;
            OS_ANDROID = 1;
            OS_IOS		= 2;
            OS_WP 		= 3;*/
          var deviceIdsArr = device_ids.split(";")
          var imei = ""
          var androidId = ""
          var idfa = ""
          deviceIdsArr.foreach{
            x =>
              var arr = x.split(":")
              if(arr.length == 2){
                if(arr(0) == "DEVID_IMEI"){
                  imei = arr(1)
                }else if(arr(0) == "DEVID_ANDROIDID"){
                  androidId = arr(1)
                }else if(arr(0) == "DEVID_IDFA"){
                  idfa = arr(1)
                }
              }
          }
          (media_appsid, adslotid, adslot_type, model, brand,os , imei, androidId,idfa, city)
      }.filter{
      case (media_appsid, adslotid, adslot_type, model, brand,os , imei, androidId,idfa, city) =>
        if(os == 1 && imei.length > 0 ){
          true
          /*}else if (os == 2 && idfa.length >0){
             true*/
        }else{
          false
        }
    }.map{
      case (media_appsid, adslotid, adslot_type, model, brand,os , imei, androidId,idfa, city) =>
        ((media_appsid, adslotid, adslot_type, imei),(model, brand, os, imei, androidId, idfa, city))
    }.reduceByKey((x, y) => x)

    var diffAndroidId =  union1.leftOuterJoin(union2).map{
      case  ((media_appsid, adslotid, adslot_type, imei),((model1, brand1, os1, imei1, androidId1, idfa1, city1),
      other: Option[(String, String, Int, String, String, String, Int)])) =>
        var other2 = other.getOrElse(null)
        var flagAndroid = 0
        var flagModel = 0
        var flagBrand = 0
        var flagCity = 0

        if(other2 != null && androidId1 != other2._5){
          flagAndroid = 1
        }
        if(other2 != null && model1 != other2._1){
          flagModel = 1
        }
        if(other2 != null && brand1 != other2._2){
          flagBrand = 1
        }
        if(other2 != null && city1 != other2._7){
          flagCity = 1
        }
        var sameImei = 0
        if(other2 != null){
            sameImei = 1
        }
        ((media_appsid, adslotid, adslot_type),(1,sameImei, flagAndroid, flagBrand, flagModel, flagCity))
    }.reduceByKey((x, y) => (x._1+y._1,x._2+y._2 ,x._3+y._3,x._4+y._4,x._5+y._5,x._6+y._6)).map{
     case ((media_appsid, adslotid, adslot_type),(allImei, sameImei, flagAndroid, flagBrand, flagModel,flagCity))=>
       media_appsid+","+adslotid+","+adslot_type+","+allImei+","+sameImei+","+flagAndroid+","+flagBrand+","+flagModel + ","+flagCity
   }
  /*  var toResult =  unionAll.leftOuterJoin(diffAndroidId).map{
      case ((media_appsid, adslotid, adslot_type),(request, imei: Option[(Int, Int, Int, Int, Int)]))=>
        var imeiObj = imei.getOrElse((0,0,0,0,0))
        media_appsid+","+adslotid+","+adslot_type+","+request+","+imeiObj._1+","+imeiObj._2+","+imeiObj._3+","+imeiObj._4+","+imeiObj._5
    }*/
    diffAndroidId.collect().foreach(println)

   /*var diffAndroidId =  union1.join(union2).map{
      case  ((media_appsid, adslotid, adslot_type, imei),((model1, brand1, os1, imei1, androidId1, idfa1, city1),(model2, brand2, os2, imei2, androidId2, idfa2, city2))) =>
        var flagAndroid = 0
        var flagModel = 0
        var flagBrand = 0
        var flagCity = 0
        if(androidId1 != androidId2){
          flagAndroid = 1
        }
        if(model1 != model2){
          flagModel = 1
        }
        if(brand1 != brand2){
          flagBrand = 1
        }
        if(city1 != city2){
          flagCity = 1
        }
        ((media_appsid, adslotid, adslot_type),(1, flagAndroid, flagBrand, flagModel, flagCity))
    }.reduceByKey((x, y) => (x._1+y._1,x._2+y._2 ,x._3+y._3,x._4+y._4,x._5+y._5))/*.map{
     case ((media_appsid, adslotid, adslot_type),(count, flagAndroid, flagBrand, flagModel))=>
      // media_appsid+","+adslotid+","+adslot_type+","+count+","+flagAndroid+","+flagBrand+","+flagModel
   }*/
   var toResult =  unionAll.leftOuterJoin(diffAndroidId).map{
      case ((media_appsid, adslotid, adslot_type),(request, imei: Option[(Int, Int, Int, Int, Int)]))=>
        var imeiObj = imei.getOrElse((0,0,0,0,0))
        media_appsid+","+adslotid+","+adslot_type+","+request+","+imeiObj._1+","+imeiObj._2+","+imeiObj._3+","+imeiObj._4+","+imeiObj._5
    }
    toResult.collect().foreach(println)*/
  }
}