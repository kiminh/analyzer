package com.cpc.spark.log.anal

import java.sql.{DriverManager, ResultSet}
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.mutable


/**
  * 在TopCtrIdeaV2基础上做修改
  * 1. 添加推荐素材新类型：(已有：1为小图，2为长图，3组图)4为视频，6为文本 7互动 8开屏 9 横幅
  * 2. 删除就系统的数据：adv_old
  * 3. 删除ctr等比缩放
  * 4. 根据adslot_type等比取80000数据
  *
  * created by zhy
  */
object TopCtrIdeaV2 {

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println(
        s"""
           |Usage: GetUserProfile <day_before> <int> <table:string>
           |
        """.stripMargin)
      System.exit(1)
    }

    Logger.getRootLogger.setLevel(Level.WARN)
    val dayBefore = args(0).toInt //10
    val table = args(1) //top_ctr_idea

    val spark = SparkSession.builder()
      .appName("top ctr ideas")
      .enableHiveSupport()
      .getOrCreate()

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -dayBefore)

    //读取近10天广告信息。 ((adslot_type, ideaid), Adinfo)
    var adctr: DataFrame = null
    for (i <- 0 until dayBefore) {
      val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      // 去掉金币样式,增加对媒体id的group by
      val stmt =
        """
          |select adslot_type, ideaid, media_appsid, sum(isclick) as sum_click, sum(isshow) as sum_show
          |from dl_cpc.cpc_basedata_union_events where `day` = "%s" and isshow = 1
          |and adslot_id > 0 and ideaid > 0 and exp_style <> 510127
          |group by adslot_type, ideaid, media_appsid
        """.stripMargin.format(date)
      println(stmt)
      val ulog = spark.sql(stmt)

      if (adctr == null) {
        adctr = ulog
      } else {
        adctr = adctr.union(ulog) //合并
      }
      cal.add(Calendar.DATE, 1)
    }

    val adinfo = adctr
      .groupBy("adslot_type", "ideaid", "media_appsid")
      .agg(
        expr("sum(sum_click)").cast("int").alias("click"),
        expr("sum(sum_show)").cast("int").alias("show")
        // expr("sum(sum_click)/sum(sum_show)*1000000").cast("int").alias("ctr")
      )
      .where("click>0")
      .where("show>1000") // 对所有栏位类型进行大于1000展示量过滤
      .rdd
      .map { r =>
        val idea_id = r.getAs[Int]("ideaid")
        val adslot_type = r.getAs[Int]("adslot_type")
        val media_appsid = r.getAs[String]("media_appsid")
        val media_id = media_appsid match {
          case "80000001" | "80000002" => 1   // 趣头条
          case "80001098" | "80001292" | "80001539" | "80002480" | "80001011" | "80004786" | "80004787" => 4    // 米读
          case "80002819" => 5  // 段子
          case "80003865" => 6  // 趣键盘
          case _ => 2 // 联盟头部
        }
        val click = r.getAs[Int]("click")
        val show = r.getAs[Int]("show")
         // val ctr = r.getAs[Int]("ctr")
        val v = Adinfo(
          adslot_type = adslot_type,
          idea_id = idea_id,
          media_id = media_id,
          show = show,
          click = click)
          // ctr = ctr)
          // v
          ((adslot_type, idea_id, media_id), v)
      }
        .reduceByKey((x, y) => Adinfo(
            adslot_type = x.adslot_type,
            idea_id = x.idea_id,
            media_id = x.media_id,
            show = x.show + y.show,
            click = x.click + y.click ))
        .map(x => x._2)
      .toLocalIterator
      .toSeq

    /**
      * 计算ctr. ctr=click/show*1e6
      * 返回 Adinfo
      */

    val ub = getUserBelong() //获取广告主id, 代理账户id  Map[id, belong]
    val titles = getIdeaTitle() //从adv.idea表读取数据  Map[id, (title, image,type,video_id,user_id,category)]
    // val imgs = getIdaeImg() //从adv.resource表读取素材资源  Map[id, (remote_url, type)]

    println("总条数： " + adinfo.size)
    println("title length: " + titles.size)
    // println("imgs length: " + imgs.size)

    var id = 0
    val topIdeaRDD = adinfo
      .map {
        x => //Adinfo
          val ad = titles.getOrElse(x.idea_id, null) //根据ideaid获得  (title,image,type,video_id,user_id,category)
          if (ad != null) {
            var mtype = ad._3 //type

            // var img: Seq[(String, Int)] = Seq()
            // if (mtype != 6) {
            //   img = ad._2.split(",").map(_.toInt).toSeq
            //     .map(x => imgs.getOrElse(x, null)).filter(_ != null) //获得image的type和remote_url (remote_url,type)
            // }
            // val video = imgs.getOrElse(ad._4, null) //获得video的type和remote_url (remote_url,type)


            val adclass = (ad._6 / 1000000) * 1000000 + 100100
            var image_ids = ""
            if (mtype == 4) {
              image_ids = ad._4.toString()
            } else {
              image_ids = ad._2
            }

            id += 1
            var topIdea = TopIdea(
              id = id,
              user_id = ad._5,
              idea_id = x.idea_id,
              agent_id = ub.getOrElse(ad._5, 0),
              adclass = ad._6,
              adclass_1 = adclass,
              adslot_type = x.adslot_type,
              media_id = x.media_id,
              title = ad._1, //title
              image_ids = image_ids,
              mtype = mtype, //type
              from = "cpc_adv",
              show = x.show,
              click = x.click,
              ctr_score = x.click / x.show * 1000000
            )

            // if (mtype == 4) { //视频
            //   topIdea = topIdea.copy(image_ids = video._1)
            // } else { //除视频以外其它
            //   topIdea = topIdea.copy(images = img.map(_._1).mkString(" "))
            // }

            topIdea

          } else {
            null
          }
      }
      .filter(x => x != null && !(x.adclass == 118100100 && x.mtype == 4) &&
        !Seq(1537507, 1540081, 1544700, 1544832, 1545016, 1545020, 1545025, 1545197, 1545200, 1545202, 1547033, 1549325, 1549327, 1549328, 1549329,
          1549342, 1552330, 1552333, 1552336, 1552339, 1552340, 1552545, 1552551, 1552553, 1552562, 1552565, 1001028, 1501897).contains(x.user_id) &&
        !(
          (110110100 == x.adclass && isStringCotainsSomeChars(x.title, Seq("千", "万", "十万", "百万", "1000", "10000", "买车", "买房", "暴富", "马云"))) ||
            (125100100 == x.adclass && isStringCotainsSomeChars(x.title, Seq("千", "万", "十万", "百万", "1000", "10000", "买车", "买房", "暴富", "马云"))) ||
            (100101109 == x.adclass && isStringCotainsSomeChars(x.title, Seq("提现", "现金", "充值", "真金")))
          )
      ) //推荐素材去掉test用户 1001028 1501897； 过滤违规关键词

    //!Seq(110100100, 110110100, 125100100, 100100100, 100101100, 100101109).contains(x.adclass)
    val sum = topIdeaRDD.size.toDouble //总元素个数
    println("总元素个数：" + sum)

    val adslot_type: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7)
    var rate_map: mutable.Map[Int, Double] = mutable.HashMap() //占比; k-adslot_type,v-占比
    var max_ctr_map: mutable.Map[Int, Int] = mutable.HashMap() //最大ctr; k-adslot_type,v-最大ctr
    var adslot_type_map: mutable.Map[Int, Int] = mutable.HashMap() //每个adslot_type总元素个数; k-adslot_type,v-元素个数

    for (i <- adslot_type) {
      var tmp = topIdeaRDD.filter(_.adslot_type == i)

      println("adslot_type=" + i + "有 " + tmp.length + " 条")
      adslot_type_map.put(i, tmp.length)

      if (tmp.length > 0) {
        //计算占比
        var r = tmp.length / sum
        rate_map.put(i, r)

        //计算最大ctr
        var max_ctr = tmp.map(_.ctr_score).max
        max_ctr_map.put(i, max_ctr)
      }

    }

    println("占比：" + rate_map)

    for ((x, y) <- max_ctr_map) {
      println("adslot_type: " + x + "; max_ctr: " + y)
    }

    for (i <- adslot_type) {
      println("adslot_type=" + i + "取 " + (80000 * (rate_map.getOrElse[Double](i, 0.0))).toInt + " 条")
    }


    var topIdeaData = mutable.Seq[TopIdea]()
    var topIdeaRDD2: Seq[TopIdea] = Seq()

    for (i <- adslot_type) {
      val size = (80000 * (rate_map.getOrElse[Double](i, 0.0))).toInt //要取元素个数
      val size2 = adslot_type_map.getOrElse[Int](i, 0) //每个adslot_type总元素个数

      //如果要取元素个数大于总元素个数，或总元素个数小于500，取所有
      if (size > size2 || size2 < 500) {
        topIdeaRDD2 = topIdeaRDD.filter(x => x.adslot_type == i)
          .sortWith(_.ctr_score > _.ctr_score).take(size2)
      } else {
        topIdeaRDD2 = topIdeaRDD.filter(x => x.adslot_type == i)
          .sortWith(_.ctr_score > _.ctr_score).take(size)
      }

      println("adslot_type=" + i + "已取出 " + topIdeaRDD2.length + " 条")
      topIdeaData = topIdeaData ++ topIdeaRDD2
    }


    if (topIdeaData.length > 0) {
      println("###### res: " + topIdeaData(0))
    }

    val conf = ConfigFactory.load()
    val mariadbUrl = conf.getString("mariadb.adv_report.url")
    val mariadbProp = new Properties()
    mariadbProp.put("user", conf.getString("mariadb.adv_report.user"))
    mariadbProp.put("password", conf.getString("mariadb.adv_report.password"))
    mariadbProp.put("driver", conf.getString("mariadb.adv_report.driver"))

    //truncate table
    try {
      Class.forName(mariadbProp.getProperty("driver"))
      val conn = DriverManager.getConnection(
        mariadbUrl,
        mariadbProp.getProperty("user"),
        mariadbProp.getProperty("password"))
      val stmt = conn.createStatement()
      val sql =
        """
          |TRUNCATE TABLE adv_report.%s
        """.stripMargin.format(table)
      stmt.executeUpdate(sql);
      stmt.close()
      conn.close()
    } catch {
      case e: Exception => println("truncate table failed : " + e);
    }

    spark.createDataFrame(topIdeaData)
      .drop("adslot_type", "show", "click")
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report." + table, mariadbProp)

    /* 插入手动推荐的素材 --陈超 */
    try {
      Class.forName(mariadbProp.getProperty("driver"))
      val conn = DriverManager.getConnection(
        mariadbUrl,
        mariadbProp.getProperty("user"),
        mariadbProp.getProperty("password"))
      val stmt = conn.createStatement()
      val sql_insert =
        s"""
           |INSERT INTO `report`.%s (`agent_id`, `user_id`, `idea_id`, `adclass`, `adclass_1`, `title`, `mtype`, `images`, `ctr_score`, `from`) VALUES (1000003, 1000002, '1953639', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (1).gif', 21848, 'cpc_adv'), (1000003, 1000002, '1607163', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (1).jpg', 71297, 'cpc_adv'), (1000003, 1000002, '1933010', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (10).gif', 112170, 'cpc_adv'), (1000003, 1000002, '1569289', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (11).gif', 147245, 'cpc_adv'), (1000003, 1000002, '1933013', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (12).gif', 106979, 'cpc_adv'), (1000003, 1000002, '1953639', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (13).gif', 148176, 'cpc_adv'), (1000003, 1000002, '1607162', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (14).gif', 58631, 'cpc_adv'), (1000003, 1000002, '1648200', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (15).gif', 55726, 'cpc_adv'), (1000003, 1000002, '1594572', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (16).gif', 3962, 'cpc_adv'), (1000003, 1000002, '1594614', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (17).gif', 131729, 'cpc_adv'), (1000003, 1000002, '1714728', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (18).gif', 80458, 'cpc_adv'), (1000003, 1000002, '1648200', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (19).gif', 6200, 'cpc_adv'), (1000003, 1000002, '1594614', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (2).gif', 49995, 'cpc_adv'), (1000003, 1000002, '1594572', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (2).jpg', 58845, 'cpc_adv'), (1000003, 1000002, '1646182', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (20).gif', 133804, 'cpc_adv'), (1000003, 1000002, '1555287', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (21).gif', 42934, 'cpc_adv'), (1000003, 1000002, '1813161', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (22).gif', 131831, 'cpc_adv'), (1000003, 1000002, '1607160', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (23).gif', 54403, 'cpc_adv'), (1000003, 1000002, '1607162', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (24).gif', 44261, 'cpc_adv'), (1000003, 1000002, '1607162', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (25).gif', 13918, 'cpc_adv'), (1000003, 1000002, '1813169', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (26).gif', 14341, 'cpc_adv'), (1000003, 1000002, '1714728', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (27).gif', 139885, 'cpc_adv'), (1000003, 1000002, '1569114', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (28).gif', 110873, 'cpc_adv'), (1000003, 1000002, '1813169', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (29).gif', 65812, 'cpc_adv'), (1000003, 1000002, '2326306', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (3).gif', 122163, 'cpc_adv'), (1000003, 1000002, '1823313', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (3).jpg', 99336, 'cpc_adv'), (1000003, 1000002, '1594572', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (30).gif', 89187, 'cpc_adv'), (1000003, 1000002, '1594572', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (31).gif', 138798, 'cpc_adv'), (1000003, 1000002, '1646182', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (32).gif', 126034, 'cpc_adv'), (1000003, 1000002, '1569292', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (33).gif', 133063, 'cpc_adv'), (1000003, 1000002, '1594614', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (34).gif', 92821, 'cpc_adv'), (1000003, 1000002, '1714728', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (35).gif', 33353, 'cpc_adv'), (1000003, 1000002, '1648200', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (36).gif', 133961, 'cpc_adv'), (1000003, 1000002, '1813169', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (37).gif', 100940, 'cpc_adv'), (1000003, 1000002, '2144124', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (38).gif', 146980, 'cpc_adv'), (1000003, 1000002, '1594740', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (39).gif', 89101, 'cpc_adv'), (1000003, 1000002, '1615483', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (4).gif', 54233, 'cpc_adv'), (1000003, 1000002, '1993322', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (40).gif', 22142, 'cpc_adv'), (1000003, 1000002, '1823313', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (41).gif', 145392, 'cpc_adv'), (1000003, 1000002, '1813161', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (42).gif', 143942, 'cpc_adv'), (1000003, 1000002, '1555287', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (43).gif', 120704, 'cpc_adv'), (1000003, 1000002, '1569289', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (44).gif', 15070, 'cpc_adv'), (1000003, 1000002, '1594614', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (45).gif', 142998, 'cpc_adv'), (1000003, 1000002, '1569191', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (46).gif', 111307, 'cpc_adv'), (1000003, 1000002, '1607161', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (47).gif', 44012, 'cpc_adv'), (1000003, 1000002, '1615483', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (48).gif', 57153, 'cpc_adv'), (1000003, 1000002, '1953639', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (49).gif', 70206, 'cpc_adv'), (1000003, 1000002, '1607163', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (5).gif', 59875, 'cpc_adv'), (1000003, 1000002, '1594614', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (50).gif', 95314, 'cpc_adv'), (1000003, 1000002, '1813169', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (51).gif', 119052, 'cpc_adv'), (1000003, 1000002, '1813169', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (52).gif', 56878, 'cpc_adv'), (1000003, 1000002, '1944679', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (53).gif', 115883, 'cpc_adv'), (1000003, 1000002, '1677143', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (54).gif', 6554, 'cpc_adv'), (1000003, 1000002, '1813189', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (55).gif', 87389, 'cpc_adv'), (1000003, 1000002, '1944791', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (56).gif', 118144, 'cpc_adv'), (1000003, 1000002, '1648200', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (57).gif', 62397, 'cpc_adv'), (1000003, 1000002, '1944791', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (58).gif', 49610, 'cpc_adv'), (1000003, 1000002, '1531704', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (59).gif', 73881, 'cpc_adv'), (1000003, 1000002, '1813169', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (6).gif', 22529, 'cpc_adv'), (1000003, 1000002, '1531704', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (60).gif', 38291, 'cpc_adv'), (1000003, 1000002, '2326306', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (61).gif', 75621, 'cpc_adv'), (1000003, 1000002, '1933010', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (62).gif', 126823, 'cpc_adv'), (1000003, 1000002, '1944791', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (63).gif', 119973, 'cpc_adv'), (1000003, 1000002, '1933010', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (64).gif', 47111, 'cpc_adv'), (1000003, 1000002, '1944679', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (65).gif', 6459, 'cpc_adv'), (1000003, 1000002, '2326306', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (66).gif', 4899, 'cpc_adv'), (1000003, 1000002, '1531704', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (67).gif', 61837, 'cpc_adv'), (1000003, 1000002, '1607160', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (68).gif', 109574, 'cpc_adv'), (1000003, 1000002, '1953639', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (69).gif', 55667, 'cpc_adv'), (1000003, 1000002, '1607163', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (7).gif', 127761, 'cpc_adv'), (1000003, 1000002, '1714728', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (70).gif', 100649, 'cpc_adv'), (1000003, 1000002, '1993322', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (71).gif', 30624, 'cpc_adv'), (1000003, 1000002, '1607160', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (72).gif', 144934, 'cpc_adv'), (1000003, 1000002, '1594785', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (73).gif', 113500, 'cpc_adv'), (1000003, 1000002, '1594785', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (74).gif', 128220, 'cpc_adv'), (1000003, 1000002, '1594614', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (75).gif', 36377, 'cpc_adv'), (1000003, 1000002, '1569289', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (76).gif', 128772, 'cpc_adv'), (1000003, 1000002, '2326306', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (77).gif', 102770, 'cpc_adv'), (1000003, 1000002, '1953639', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (78).gif', 78947, 'cpc_adv'), (1000003, 1000002, '1607162', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (79).gif', 8472, 'cpc_adv'), (1000003, 1000002, '1646182', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (8).gif', 86996, 'cpc_adv'), (1000003, 1000002, '1933013', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (80).gif', 129537, 'cpc_adv'), (1000003, 1000002, '1594785', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (81).gif', 56053, 'cpc_adv'), (1000003, 1000002, '1607160', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (9).gif', 55698, 'cpc_adv')
        """.stripMargin.format(table)
      stmt.executeUpdate(sql_insert);
      stmt.close()
      conn.close()
    } catch {
      case e: Exception => println("insert table failed : " + e);
    }
    println("###### num: " + topIdeaData.length)
  }

  def getUserBelong(): Map[Int, Int] = {
    val sql = "select id,belong from user" //广告主id, 代理账户id
    val rs = getAdDbResult("mariadb.adv", sql)
    val ub = mutable.Map[Int, Int]()
    while (rs.next()) {
      ub.update(rs.getInt("id"), rs.getInt("belong"))
    }

    ub.toMap
  }

  def getIdeaTitle(): Map[Int, (String, String, Int, Int, Int, Int)] = {
    //荐素材类型: 1为小图，2为长图，3组图，4为视频，6为文本 7互动 8开屏 9 横幅
    var sql = "select id, title, image, type, video_id, user_id, category from idea where type in (1,2,3,4,6,7,8,9)"  // 去掉了action_type=1的过滤
    val ideas = mutable.Map[Int, (String, String, Int, Int, Int, Int)]()
    var rs = getAdDbResult("mariadb.adv", sql)
    while (rs.next()) {
      val idea = (rs.getString("title"), rs.getString("image"),
        rs.getInt("type"), rs.getInt("video_id"),
        rs.getInt("user_id"), rs.getInt("category"))
      val id = rs.getInt("id")
      ideas.update(id, idea)
    }

    ideas.toMap
  }

  def getIdaeImg(): Map[Int, (String, Int)] = {
    val sql = "select id,`type`,remote_url from resource"
    val images = mutable.Map[Int, (String, Int)]()
    var rs = getAdDbResult("mariadb.adv", sql)
    while (rs.next()) {
      images.update(rs.getInt("id"), (rs.getString("remote_url"), rs.getInt("type")))
    }

    images.toMap
  }

  def getAdDbResult(confKey: String, sql: String): ResultSet = {
    val conf = ConfigFactory.load()
    val mariadbProp = new Properties()
    mariadbProp.put("url", conf.getString(confKey + ".url"))
    mariadbProp.put("user", conf.getString(confKey + ".user"))
    mariadbProp.put("password", conf.getString(confKey + ".password"))
    mariadbProp.put("driver", conf.getString(confKey + ".driver"))

    Class.forName(mariadbProp.getProperty("driver"))
    val conn = DriverManager.getConnection(
      mariadbProp.getProperty("url"),
      mariadbProp.getProperty("user"),
      mariadbProp.getProperty("password"))
    val stmt = conn.createStatement()
    stmt.executeQuery(sql)
  }

  def isStringCotainsSomeChars(str: String, seq: Seq[String]): Boolean = {
    var b: Boolean = false
    for (s <- seq) {
      if (str.contains(s)) {
        b = true
      }
    }
    b
  }

  private case class Adinfo(
                             agent_id: Int = 0,
                             user_id: Int = 0,
                             idea_id: Int = 0,
                             adclass: Int = 0,
                             adslot_type: Int = 0,
                             media_id: Int = 0,
                             click: Int = 0,
                             show: Int = 0
                             // ctr: Int = 0,
                             // ctr_type: Int = 0
                           ) {

  }

  private case class TopIdea(
                              id: Int = 0,
                              agent_id: Int = 0,
                              user_id: Int = 0,
                              idea_id: Int = 0,
                              adclass: Int = 0,
                              adclass_1: Int = 0, //一级行业
                              adslot_type: Int = 0,
                              media_id: Int = 0,
                              title: String = "",
                              mtype: Int = 0,
                              image_ids: String = "",
                              ctr_score: Int = 0,
                              from: String = "",
                              click: Int = 0,
                              show: Int = 0
                            )

}
