package com.cpc.spark.coin

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Calendar

import autoCoin.autoCoin.{AutoCoin, Coin}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author Jinbao
  * @date 2019/3/5 16:21
  */
object AutoCoinStrategy {
    def main(args: Array[String]): Unit = {

        val cpc_api_union_log = args(0) //"dl_cpc.cpc_api_union_log"

        val ml_cvr_feature_v1 = args(1) //"dl_cpc.ml_cvr_feature_v1"

        val coinTable = "dl_cpc.cpc_auto_coin_idea_threshold_record"    //记录ideaid的cvr阈值

        val date = args(2)

        val hour = args(3)

        val p1 = args(4).toDouble       //非黑五类的阈值，0.9

        val p2 = args(5).toDouble       //黑五类的阈值，0.7

        val preDay = args(6).toInt      //根据几天的数据统计，7

        val spark = SparkSession.builder()
          .appName(s"AutoCoinStrategy date = $date, hour = $hour")
          .enableHiveSupport()
          .getOrCreate()

        import spark.implicits._

        //黑五类userid名单
        val unKnownUseridList =
            """
              |0
            """.stripMargin


        val unKnownIdeaidList = getUnKnownIdeaidList(spark,unKnownUseridList) //黑五类ideaid名单

        val dateHourFilter = getDateHourFilter(date,hour,preDay)    //获取时间过滤条件

        val lowAucUseridFilter = getLowAucUseridFilter(spark,date)  //根据昨天报表中的auc过滤auc低于0.5的userid

        println(lowAucUseridFilter)

        //ideaid黑名单，不出自动金币
        val ideaBlacklist =
            """
              |2391911, 2385002, 2388289, 2391495, 2381868, 2391641, 2330249, 2384970, 2391533, 2360176, 2391895, 2391881, 2390834
            """.stripMargin

        //正企userid白名单，出自动金币
        val userWhiteList =
            """
              |0
            """.stripMargin

        val mlFeatureNth = calcThreshold(spark,ml_cvr_feature_v1,dateHourFilter,ideaBlacklist,
            lowAucUseridFilter,userWhiteList,unKnownIdeaidList,
            p1,p2,
            date,hour)
        val apiUnionNth = calcApiThreshold(spark,cpc_api_union_log,dateHourFilter.replace("date","day"),ideaBlacklist,
            lowAucUseridFilter,userWhiteList,unKnownIdeaidList,
            p1,p2,
            date,hour)
        println("mlFeatureNth 's count" + mlFeatureNth.count())
        println("apiUnionNth 's count" + apiUnionNth.count())
        val Nth = mlFeatureNth.fullOuterJoin(apiUnionNth)   //两个数据组合
          .map(x => {
            val label = x._2._1.orNull

            val api = x._2._2.orNull
            if (label != null && api != null) {
                coin(ideaid = x._1,
                    label_exp_cvr = label._1,
                    label_min = label._2,
                    label_max = label._3,
                    label_num = label._4,
                    label_5th = label._5,
                    label_6th = label._6,
                    label_7th = label._7,
                    label_8th = label._8,
                    label_9th = label._9,

                    api_exp_cvr = api._1,
                    api_min = api._2,
                    api_max = api._3,
                    api_num = api._4,
                    api_5th = api._5,
                    api_6th = api._6,
                    api_7th = api._7,
                    api_8th = api._8,
                    api_9th = api._9,

                    date = date,
                    hour = hour.toString)
            }
            else if (label == null && api != null) {
                coin(ideaid = x._1,

                    api_exp_cvr = api._1,
                    api_min = api._2,
                    api_max = api._3,
                    api_num = api._4,
                    api_5th = api._5,
                    api_6th = api._6,
                    api_7th = api._7,
                    api_8th = api._8,
                    api_9th = api._9,

                    date = date,
                    hour = hour.toString)
            }
            else if (label != null && api == null) {
                coin(ideaid = x._1,
                    label_exp_cvr = label._1,
                    label_min = label._2,
                    label_max = label._3,
                    label_num = label._4,
                    label_5th = label._5,
                    label_6th = label._6,
                    label_7th = label._7,
                    label_8th = label._8,
                    label_9th = label._9,

                    date = date,
                    hour = hour.toString)
            }
            else {
                coin(ideaid = x._1,
                    date = date,
                    hour = hour.toString)
            }
        })
          .filter(x => x.api_num >= 30 || x.label_num >= 30)    //过滤数据量小于等于30
          .toDS()
          .coalesce(1)

        println("Nth 's count is " + Nth.count())

        Nth.write.mode("overwrite").insertInto(coinTable)

        //保存到PB文件

        val autoCoinListBuffer = scala.collection.mutable.ListBuffer[AutoCoin]()

        Nth.collect().foreach(coin => {
            autoCoinListBuffer += AutoCoin(
                ideaid = coin.ideaid,
                label2ExpCvr = coin.label_exp_cvr,
                apiExpCvr = coin.api_exp_cvr)
        })

        val autoCoinList = autoCoinListBuffer.toArray
        println("autoCoinList 's num is " + autoCoinList.length)
        val coinData = Coin(coin = autoCoinList)

        coinData.writeTo(new FileOutputStream("Coin.pb"))

        println("write to Coin.pb success")

    }

    /**
      * 根据userid获取其对应的所有ideaid
      * @param spark
      * @param unKnownUseridList
      * @return
      */
    def getUnKnownIdeaidList(spark:SparkSession, unKnownUseridList:String): String = {
        val getIdeaidSql =
            s"""
               |select id as ideaid
               |from src_cpc.cpc_idea
               |where user_id in ($unKnownUseridList)
             """.stripMargin

        println(getIdeaidSql)

        val unKnownIdeaid = spark.sql(getIdeaidSql)
          .rdd
          .map(_.getAs[Int]("ideaid").toString)
          .collect()
          .toList

        val l0 = List("0")

        val unKnownIdeaidList = unKnownIdeaid:::l0 //增加一个默认值，防止空list

        unKnownIdeaidList.mkString(",")
    }

    /**
      * 获取时间过滤条件
      * @param date
      * @param hour
      * @param preDay
      * @return
      */
    def getDateHourFilter(date:String,hour:String,preDay: Int): String = {
        val datehourlist = scala.collection.mutable.ListBuffer[String]()
        val cal = Calendar.getInstance()
        cal.set(date.substring(0, 4).toInt, date.substring(5, 7).toInt - 1, date.substring(8, 10).toInt, hour.toInt, 0)
        for (t <- 0 to 24 * preDay) {
            if (t > 0) {
                cal.add(Calendar.HOUR, -1)
            }
            val sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            val dd = sf.format(cal.getTime())
            val d1 = dd.substring(0, 10)
            val h1 = dd.substring(11, 13)
            val datecond = s"(`date` = '$d1' and hour = '$h1')"
            if (!(d1 == "2019-02-18" || d1 == "2019-02-17" || d1== "2019-02-16"))
                datehourlist += datecond
        }
        datehourlist.toList.mkString(" or ")
    }

    def getPreTime(date:String,hour:String,pre:Int):String = {
        val cal = Calendar.getInstance()
        cal.set(date.substring(0, 4).toInt, date.substring(5, 7).toInt - 1, date.substring(8, 10).toInt, hour.toInt, 0)
        cal.add(Calendar.HOUR, -pre)
        val sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val dd = sf.format(cal.getTime())
        dd
    }

    /**
      * 根据昨天的报表dl_cpc.cpc_report_coin_userid_metrics，获取auc<=0.5的userid列表
      * @param spark
      * @param date
      * @return
      */
    def getLowAucUseridFilter(spark:SparkSession,date:String):String = {
        val metricsSql =
            s"""
               |select userid
               |from dl_cpc.cpc_report_coin_userid_metrics
               |where `date`=date_sub('$date',1)
               |and auc <= 0.5
               |and auc >= 0
             """.stripMargin

        val lowAucUserid = spark.sql(metricsSql)
          .rdd
          .map(x => x.getAs[Int]("userid").toString)
          .collect()
          .toList
        val l0 = List("0")

        val lowAucUseridFilter = lowAucUserid:::l0  //增加一个默认值，防止空list

        lowAucUseridFilter.mkString(",")
    }

    /**
      * api计算
      * @param spark
      * @param cpc_api_union_log
      * @param dateHourFilter
      * @param ideaBlacklist
      * @param lowAucUseridFilter
      * @param userWhiteList
      * @param unKnownIdeaidList
      * @param p1
      * @param p2
      * @return
      */
    def calcApiThreshold(spark:SparkSession, cpc_api_union_log:String, dateHourFilter:String, ideaBlacklist:String,
                         lowAucUseridFilter:String, userWhiteList:String, unKnownIdeaidList:String,
                         p1:Double, p2:Double, date:String, hour:String):RDD[(Int, (Int, Int, Int, Int, Int, Int, Int, Int, Int))] = {
        val apiUnionLogSql =
            s"""
               |select ideaid,exp_cvr
               |from dl_cpc.api_basedata_union_events
               |where ($dateHourFilter)
               |and media_appsid in ('80000001','80000002')
               |and ideaid > 0
               |and adslot_type in (1, 2)
               |and round(adclass/1000000) != 107 and round(adclass/1000000) != 134
               |and ((adslot_type<>7 and adclass like '100%') or (adclass in (110110100, 125100100)))
               |and ideaid not in ($ideaBlacklist)
               |and userid not in ($lowAucUseridFilter)
               |and (userid in ($userWhiteList) or usertype != 2)
             """.stripMargin
        println(apiUnionLogSql)

        val apiUnionLog = spark.sql(apiUnionLogSql).cache()

        val apiUnionLog1 = apiUnionLog.filter(s"ideaid not in ($unKnownIdeaidList)")    //非黑五类ideaid
        val apiUnionLog2 = apiUnionLog.filter(s"ideaid in ($unKnownIdeaidList)")    //黑五类ideaid
        //非黑五类按p1值计算，黑五类按p2值计算，最后合并
        val apiUnionNth = getThresholdAdjust(spark,apiUnionLog1,date,hour, p1).union(getThresholdUnAdjust(apiUnionLog2, p2))

        apiUnionLog.unpersist()

        apiUnionNth
    }

    def calcThreshold(spark:SparkSession, ml_cvr_feature_v1:String, dateHourFilter:String, ideaBlacklist:String,
                      lowAucUseridFilter:String, userWhiteList:String, unKnownIdeaidList:String,
                      p1:Double, p2:Double, date:String, hour:String):RDD[(Int, (Int, Int, Int, Int, Int, Int, Int, Int, Int))] = {
        val mlFeatureSql =
            s"""
               |select a.ideaid as ideaid,exp_cvr
               |    from
               |    (
               |        select ideaid,exp_cvr
               |        from dl_cpc.ml_cvr_feature_v1
               |        where ($dateHourFilter)
               |        and label2 = 1
               |        and media_appsid in ('80000001','80000002')
               |        and adslot_type in (1, 2)
               |        and ideaid > 0
               |        and round(adclass/1000000) != 107 and round(adclass/1000000) != 134
               |        and ((adslot_type<>7 and adclass like '100%') or (adclass in (110110100, 125100100)))
               |    ) a left outer join
               |    (
               |        select x.id as ideaid ,x.user_id as userid,y.account_type as account_type
               |        from
               |        (
               |            select id,user_id
               |            from src_cpc.cpc_idea
               |            group by id,user_id
               |         ) x left outer join
               |         (
               |            select id, account_type
               |            from src_cpc.cpc_user_p
               |            where account_type = 2
               |            and day > '2019-01-01'
               |            group by id, account_type
               |         ) y
               |         on x.user_id = y.id
               |    ) b
               |    on a.ideaid = b.ideaid
               |    where
               |    a.ideaid not in ($ideaBlacklist)
               |    and b.userid not in ($lowAucUseridFilter)
               |    and (b.userid in ($userWhiteList) or b.account_type is null)
             """.stripMargin
        println(mlFeatureSql)
        val mlFeature = spark.sql(mlFeatureSql).cache()

        val mlFeature1 = mlFeature.filter(s"ideaid not in ($unKnownIdeaidList)")    //非黑五类ideaid
        val mlFeature2 = mlFeature.filter(s"ideaid in ($unKnownIdeaidList)")        //黑五类ideaid

        val mlFeatureNth = getThresholdAdjust(spark,mlFeature1,date,hour, p1).union(getThresholdUnAdjust(mlFeature2, p2))

        mlFeatureNth
    }

    /**
      * 获取ideaid自动调节阈值的p值
      * @param spark
      * @param date
      * @return
      */
    def getIdeaidP(spark:SparkSession,date:String):DataFrame = {
        val sql =
            s"""
               |select ideaid,p
               |from dl_cpc.cpc_auto_coin_idea_threshold
               |where `date`='$date'
             """.stripMargin

        val ideaP = spark.sql(sql).cache()

        ideaP
    }


    /**
      * 不自动根据反馈调节阈值，固定阈值计算
      * @param df
      * @param p
      * @return
      */
    def getThresholdUnAdjust(df: DataFrame, p: Double):RDD[(Int, (Int, Int, Int, Int, Int, Int, Int, Int, Int))] = {
        df.rdd.map(x => (x.getAs[Int]("ideaid"), x.getAs[Int]("exp_cvr")))
          .combineByKey(x => List(x),
              (x: List[Int], y: Int) => y :: x,
              (x: List[Int], y: List[Int]) => x ::: y)
          .mapValues(x => {
              val sorted = x.sorted
              val length = sorted.length - 1
              val index = (length * p).toInt
              val i5th = (length * 0.5).toInt
              val i6th = (length * 0.6).toInt
              val i7th = (length * 0.7).toInt
              val i8th = (length * 0.8).toInt
              val i9th = (length * 0.9).toInt
              val exp_cvr =  if (sorted(index) == 1000000) sorted(index) - 1 else sorted(index)
              (exp_cvr, sorted(0), sorted(sorted.length - 1), sorted.length,
                sorted(i5th), sorted(i6th), sorted(i7th), sorted(i8th), sorted(i9th))
              //x(index)
          })
    }

    /**
      * 根据反馈自动调节阈值
      * @param spark
      * @param df
      * @param date
      * @param hour
      * @param p
      * @return
      */
    def getThresholdAdjust(spark:SparkSession,df:DataFrame,date:String, hour:String, p:Double):
    RDD[(Int, (Int, Int, Int, Int, Int, Int, Int, Int, Int))] = {
        val t = getPreTime(date,hour,3) //获取3个小时前的时间

        val ideaP = getIdeaidP(spark,t.substring(0, 10))    //获取p值

        ideaP.show(10)

        df.join(ideaP,Seq("ideaid"),"left_outer").select("ideaid","exp_cvr","p")
          .na
          .fill(p,Seq("p")) //用默认p值填充
          .rdd
          .map(x => (x.getAs[Int]("ideaid"),
            (List(x.getAs[Int]("exp_cvr")),x.getAs[Double]("p"))))
          .reduceByKey((x,y) => {
              val t1 = x._1 ::: y._1
              val t2 = if (x._2 > y._2) x._2 else y._2
              (t1,t2)
          })
          .mapValues(x => {
              val cvrlist = x._1
              val p = x._2
              val sorted = cvrlist.sorted
              val length = sorted.length - 1
              val index = (length * p).toInt
              val i5th = (length * 0.5).toInt
              val i6th = (length * 0.6).toInt
              val i7th = (length * 0.7).toInt
              val i8th = (length * 0.8).toInt
              val i9th = (length * 0.9).toInt
              val exp_cvr =  if (sorted(index)==1000000) sorted(index) - 1 else sorted(index)
              (exp_cvr, sorted(0), sorted(sorted.length - 1), sorted.length,
                sorted(i5th), sorted(i6th), sorted(i7th), sorted(i8th), sorted(i9th))
          })
    }

    case class coin(var ideaid: Int = 0,
                    var label_exp_cvr: Int = 9999999,
                    var label_min: Int = 9999999,
                    var label_max: Int = 9999999,
                    var label_num: Int = 0,
                    var label_5th: Int = 9999999,
                    var label_6th: Int = 9999999,
                    var label_7th: Int = 9999999,
                    var label_8th: Int = 9999999,
                    var label_9th: Int = 9999999,

                    var api_exp_cvr: Int = 9999999,
                    var api_min: Int = 9999999,
                    var api_max: Int = 9999999,
                    var api_num: Int = 0,
                    var api_5th: Int = 9999999,
                    var api_6th: Int = 9999999,
                    var api_7th: Int = 9999999,
                    var api_8th: Int = 9999999,
                    var api_9th: Int = 9999999,
                    var date: String = "",
                    var hour: String = "")
}
