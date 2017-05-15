package com.cpc.spark.ml.parser

import com.cpc.spark.log.parser.UnionLog
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.feature.IDF

import scala.util.hashing.MurmurHash3.stringHash

/**
  * Created by Roy on 2017/5/15.
  */
object MLParser {

  def unionLogToSvm(u: UnionLog): String = {
    /*
    val isclick = x.isclick
    var svmString: String = isclick.toString
    svmString += " 1:" + x.network
    svmString += " 2:" + Math.abs(scala.util.hashing.MurmurHash3.stringHash(x.ip))
    svmString += " 3:" + x.media_type
    svmString += " 4:" + x.media_appsid
    svmString += " 5:" + x.bid
    svmString += " 6:" + x.ideaid
    svmString += " 7:" + x.unitid
    svmString += " 8:" + x.planid
    svmString += " 9:" + x.userid
    svmString += " 10:" + x.country
    svmString += " 11:" + x.province
    svmString += " 12:" + x.city
    svmString += " 13:" + x.isp
    svmString += " 14:" + Math.abs(scala.util.hashing.MurmurHash3.stringHash(x.uid))
    svmString += " 15:" + x.coin
    svmString += " 16:" + Math.abs(scala.util.hashing.MurmurHash3.stringHash(x.date))
    svmString += " 17:" + x.hour
    svmString += " 18:" + x.adslotid
    svmString += " 19:" + x.adslot_type
    svmString += " 20:" + x.adtype
    svmString += " 21:" + x.interaction
    svmString
    */

    //network
    //isp

    //planid unitid ideaid

    //interaction

    //country + city

    //hour

    //medaid
    //adslot_id


    ""
  }
}
