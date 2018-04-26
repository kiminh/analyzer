package com.cpc.spark.antispam.anal

import com.cpc.spark.ml.common.FeatureDict
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.errors

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.control.Breaks

/**
  * Created by wanli on 2017/8/4.
  */
object GetIsp {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: day <date>
        """.stripMargin)
      System.exit(1)
    }
    val date = args(0)
    Logger.getRootLogger.setLevel(Level.WARN)
    val predict = args(1).toFloat
    Logger.getRootLogger.setLevel(Level.WARN)
    val ctx = SparkSession.builder()
      .appName("model user anal" + date).config("spark.driver.maxResultSize", "20G")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", "com.cpc.spark.ml.train.LRRegistrator")
      .config("spark.kryoserializer.buffer.max", "2047MB")
      .config("spark.rpc.message.maxSize", "400")
      .config("spark.network.timeout", "240s")
      //.config("spark.speculation", "true")
      .config("spark.storage.blockManagerHeartBeatMs", "300000")
      .config("spark.scheduler.maxRegisteredResourcesWaitingTime", "100")
      .config("spark.core.connection.auth.wait.timeout", "100")
      .enableHiveSupport()
      .getOrCreate()
    var sql1 = (" SELECT distinct searchid,click_ip from dl_cpc.cpc_click_log " +
      "where `date` ='%s' and isclick=1 and antispam_rules != '' ").format(date)

    val ipDict = Source.fromFile("/data/cpc/anal/conf/ip_dict.txt", "UTF-8").getLines().filter(_.length > 0).toSeq.map{
      x =>
        var arr =  x.split(",")
      IpDict(ipToLong(arr(0)),ipToLong(arr(1)),arr(3).toLong)
    }

    val isp ="15 ,16 ,17 ,21 ,22 ,23 ,25 ,26 ,28 ,30 ,31 ,33 ,34 ,35 ,36 ,38 ,39 ,43 ,46 ,47 ,50 ,53 ,56 ,57 ,58 ,64 ,67 ,70 ,71 ,72 ,73 ,74 ,76 ,80 ,83 ,92 ,95 ,96 ,99 ,100 ,108 ,109 ,110 ,111 ,112 ,113 ,114 ,115 ,116 ,117 ,118 ,119 ,120 ,121 ,122 ,123 ,124 ,125 ,126 ,127 ,128 ,129 ,130 ,131 ,132 ,133 ,134 ,135 ,136 ,137 ,138 ,139 ,140 ,141 ,142 ,143 ,144 ,145 ,146 ,147 ,148 ,149 ,150 ,151 ,152 ,153 ,154 ,155 ,156 ,157 ,158 ,159 ,160 ,161 ,162 ,163 ,164 ,165 ,166 ,167 ,168 ,169 ,170 ,171 ,172 ,173 ,174 ,175 ,176 ,177 ,178 ,179 ,181 ,182 ,183 ,184 ,185 ,186 ,187 ,188 ,189 ,190 ,191 ,192 ,193 ,194 ,195 ,196 ,197 ,198 ,199 ,200 ,201 ,202 ,203 ,204 ,205 ,206 ,207 ,208 ,209 ,210 ,211 ,212 ,213 ,214 ,215 ,216 ,217 ,218 ,219 ,220 ,221 ,222 ,223 ,224 ,225 ,226 ,227 ,228 ,229 ,230 ,231 ,232 ,233 ,234 ,235 ,236 ,237 ,238 ,239 ,240 ,241 ,242 ,243 ,244 ,245 ,246 ,247 ,248 ,249 ,250 ,251 ,252 ,253 ,254 ,255 ,256 ,257 ,258 ,259 ,260 ,261 ,262 ,263 ,264 ,265 ,266 ,267 ,268 ,270 ,271 ,272 ,273 ,274 ,275 ,276 ,277 ,278 ,279 ,280 ,281 ,282 ,283 ,286 ,287 ,288 ,289 ,290 ,291 ,292 ,293 ,294 ,295 ,296 ,297 ,298 ,299 ,300 ,301 ,302 ,303 ,304 ,305 ,306 ,308 ,310 ,311 ,312 ,313 ,315 ,316 ,317 ,318 ,319 ,320 ,321 ,322 ,323 ,325 ,326 ,327 ,329 ,330 ,331 ,334 ,335 ,336 ,337 ,338 ,339 ,340 ,341 ,342 ,344 ,345 ,346 ,347 ,350 ,351 ,352 ,353 ,354 ,355 ,356 ,357 ,358 ,359 ,360 ,361 ,362 ,363 ,364 ,365 ,366 ,367 ,368 ,369 ,370 ,371 ,372 ,373 ,374 ,375 ,377 ,379 ,380 ,383 ,385 ,386 ,387 ,389 ,390 ,393 ,394 ,395 ,396 ,397 ,398"
    val ispDictArr = isp.split(",").map(x => x.trim.toInt)
    println(ipDict.length)
    val ipDictBroad = ctx.sparkContext.broadcast(ipDict)
    val ispDictArrBroad = ctx.sparkContext.broadcast(ispDictArr)
    println(ipDictBroad.value)
    /*var isp2 = searchIp(ipDictBroad.value,ipToLong("1.8.107.0"))
    println("isp2:"+isp2)*/
    /*
    println("sql1:"+ sql1)
   var union =  ctx.sql(sql1).rdd.map{
    x =>
      val searchid = x(0).toString()
      val click_ip = x(1).toString()
      (searchid,click_ip)
  }.map{
    case (searchid,click_ip) =>
      var isp = searchIp(ipDictBroad.value,ipToLong(click_ip))
      var flag = false
      if(isp > 0){
        ispDictArrBroad.value.foreach(x => if( x == isp ){flag = true})
      }
      (searchid, flag,isp)
   }
    union.take(10).foreach(println)
    println("count:"+ union.filter(x => x._3 == true).count())*/
}
  def searchIp(dict:Seq[IpDict], ipint:Long): Long ={
    var min = 0
    var max:Int = dict.length - 1
    var mid :Int =0
    // 导入以下包
    import scala.util.control._

    // 创建 Breaks 对象
    val loop = new Breaks

    // 在 breakable 中循环
    loop.breakable{
      // 循环
      while (min <= max ){
        mid = (min + max) / 2
        var  ipsec = dict(mid)
        if (ipsec.Start > ipint) {
          max = mid - 1
        }else if (ipsec.End < ipint) {
          min = mid + 1
        }else{
          loop.break
        }
      }
    }

    if (min > max) {
      return 0
    }
    var ipsec = dict(mid)
    if (ipsec.Start <= ipint && ipsec.End >= ipint ){
      return ipsec.Isp
    } else {
      return 0
    }
  }
  def ipToLong(ipAddress: String): Long = {
    val ipAddressInArray = ipAddress.split("\\.")
    var result:Long = 0
    var i = 0
    while (i < ipAddressInArray.length) {
       val power = 3 - i
       var ip = ipAddressInArray(i).toLong
        result =  result + ip * Math.pow(256, power).toLong
        i += 1
    }

    result
  }
  case class IpDict(
                 Start: Long = 0,
                 End: Long = 0,
                  Isp: Long = 0
                        )

}