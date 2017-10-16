package com.cpc.spark.small.tool

import java.text.SimpleDateFormat
import java.util.Calendar

import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import userprofile.Userprofile.{InterestItem, UserProfile}
import org.apache.spark.ml.linalg.{DenseVector, Vector, Vectors}

object GetAddAge {

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)

    val day = args(0).toInt
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -day)
    val dayBefore = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    val isSaveFile = args(1).toInt


    println("small tool GetAddAge run ... day %s".format(dayBefore))

    val HASHSUM = 93

    val conf = ConfigFactory.load()

    val ctx = SparkSession
      .builder()
      .appName("small tool GetAddAge")
      .enableHiveSupport()
      .getOrCreate()

    //    var xAgeData = ctx.sql(
    //      """
    //        |select qpm.member_id,qpm.device_code,qpm.birth
    //        |from gobblin.qukan_p_member_info qpm
    //        |where qpm.day="2017-10-09" AND qpm.update_time>="2017-05-01" AND qpm.device_code is not null
    //        |AND birth is not null
    //      """.stripMargin).rdd
    //      .filter(x => x(2).toString().length > 0)
    //      .map {
    //        x =>
    //          //          1: 小于18 2:18-23
    //          //          3:24-30 4:31-40
    //          //          5:41-50 6: >50
    //          var deviceid = x(1).toString()
    //          var birth = x(2).toString()
    //          var age = 0
    //          //          if (birth >= "2010-01-01") {
    //          //            age = -1
    //          //          } else if (birth >= "1998-01-01") {
    //          //            age = 1
    //          //          } else if (birth >= "1994-01-01") {
    //          //            age = 2
    //          //          } else if (birth >= "1987-01-01") {
    //          //            age = 3
    //          //          } else if (birth >= "1977-01-01") {
    //          //            age = 4
    //          //          } else if (birth >= "1967-01-01") {
    //          //            age = 5
    //          //          } else {
    //          //            age = 6
    //          //          }
    //          if (birth >= "2010-01-01") {
    //            age = -1
    //          } else if (birth >= "1990-01-01") {
    //            age = 0
    //          } else if (birth >= "1975-01-01") {
    //            age = 1
    //          } else {
    //            age = 2
    //          }
    //          //(deviceid, (age, -1, Array(1.0), Array(1.0), Array(1.0), Array(0.1), Array(0.1)))
    //          //(deviceid, age)
    //          (deviceid, (Array(1.0), Array(1.0), Array(1.0), Array(0.1)))
    //      }
    //      .filter(_._2 != -1)
    //      .map {
    //        x =>
    //          //(x._1, (Array(1.0), Array(1.0), Array(1.0), Array(0.1)))
    //          val els = new Array[Double](1)
    //          (x._1, x._2, els)
    //      }.cache()
    //    println("xAgeData count", xAgeData.count())
    //
    //    //---------------
    //    for (i <- 1 to 13) {
    //      val cal = Calendar.getInstance()
    //      cal.add(Calendar.DATE, -i)
    //      val dayBefore = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    //
    //      println("small tool GetAddAge run ... day %s".format(dayBefore))
    //
    val xMan = ctx.sql(
      """
        |SELECT DISTINCT uid
        |FROM dl_cpc.cpc_union_log
        |WHERE `date`="%s" AND age=0 AND media_appsid in ("80000001","80000002","80000006")
      """.stripMargin.format(dayBefore))
      .rdd
      .map {
        x =>
          val device = x.get(0).toString
          (device, (0.toLong, 1.toInt))
      }
      .cache()

    println("xMan count", xMan.count())

    var memberData = ctx.sql(
      """
        |select qpm.member_id,qpm.device_code
        |from gobblin.qukan_p_member_info qpm
        |where qpm.day="%s" AND qpm.update_time>="2017-05-01" AND qpm.device_code is not null
      """.stripMargin.format(dayBefore))
      .rdd
      .map {
        x =>
          val uid = x.getLong(0)
          val device = x.getString(1)
          (device, (uid, 0.toInt))
      }
    println("memberData count ", memberData.count())

    val xAgeData = xMan
      .union(memberData)
      .reduceByKey {
        (a, b) =>
          (a._1 + b._1, a._2 + b._2)
      }
      .filter {
        x =>
          (x._2._1 > 0 && x._2._2 == 1)
      }
      .map {
        x =>
          val device = x._1
          (device, (Array(1.0), Array(1.0), Array(1.0), Array(0.1)))
      }
      .repartition(50)
      .cache()
    println("xAgeData count ", xAgeData.count())


    if (isSaveFile == 1) {

      val hourData = ctx.sql(
        """
          |SELECT uid,hour,count(hour)
          |FROM dl_cpc.cpc_union_log
          |WHERE `date`="%s"
          |GROUP BY uid,hour
        """.stripMargin.format(dayBefore))
        .rdd
        .map {
          x =>
            val device = x.getString(0)
            val hour = x.getString(1)
            val hourCount = x.getLong(2).toInt
            (device, (hour, hourCount))
        }
        .groupByKey()
        .map {
          x =>
            val device = x._1
            var els = new Array[Double](24)
            x._2.map {
              y =>
                val hour = y._1
                val hourCount = y._2
                hour match {
                  case "00" => els(0) += hourCount
                  case "01" => els(1) += hourCount
                  case "02" => els(2) += hourCount
                  case "03" => els(3) += hourCount
                  case "04" => els(4) += hourCount
                  case "05" => els(5) += hourCount
                  case "06" => els(6) += hourCount
                  case "07" => els(7) += hourCount
                  case "08" => els(8) += hourCount
                  case "09" => els(9) += hourCount
                  case "10" => els(10) += hourCount
                  case "11" => els(11) += hourCount
                  case "12" => els(12) += hourCount
                  case "13" => els(13) += hourCount
                  case "14" => els(14) += hourCount
                  case "15" => els(15) += hourCount
                  case "16" => els(16) += hourCount
                  case "17" => els(17) += hourCount
                  case "18" => els(18) += hourCount
                  case "19" => els(19) += hourCount
                  case "20" => els(20) += hourCount
                  case "21" => els(21) += hourCount
                  case "22" => els(22) += hourCount
                  case "23" => els(23) += hourCount
                }
            }
            (device, (els, Array(1.0), Array(1.0), Array(0.1)))
        }

      val readData = ctx.sql(
        """
          |SELECT device,cmd,count(cmd)
          |FROM rpt_qukan.qukan_log_cmd
          |WHERE thedate='%s' AND device IS NOT NULL AND thedate IS NOT NULL
          |AND cmd IS NOT NULL
          |GROUP BY device,cmd
        """.stripMargin.format(dayBefore))
        .rdd
        .map {
          x =>
            val device = x.getString(0)
            val cmd = x.getInt(1)
            val cmdCount = x.getLong(2).toDouble
            (device, (cmd, cmdCount))
        }
        .groupByKey()
        .map {
          x =>
            val device = x._1
            val cmdArr = x._2
            var els = new Array[Double](22)
            x._2.map {
              y =>
                val cmd = y._1
                val cmdCount = y._2
                cmd match {
                  case 100 => els(0) = cmdCount
                  case 101 => els(1) = cmdCount
                  case 200 => els(2) = cmdCount
                  case 201 => els(3) = cmdCount
                  case 202 => els(4) = cmdCount
                  case 203 => els(5) = cmdCount
                  case 204 => els(6) = cmdCount
                  case 300 => els(7) = cmdCount
                  case 301 => els(8) = cmdCount
                  case 302 => els(9) = cmdCount
                  case 400 => els(10) = cmdCount
                  case 401 => els(11) = cmdCount
                  case 402 => els(12) = cmdCount
                  case 102 => els(13) = cmdCount
                  case 500 => els(14) = cmdCount
                  case 501 => els(15) = cmdCount
                  case 503 => els(16) = cmdCount
                  case 504 => els(17) = cmdCount
                  case 505 => els(18) = cmdCount
                  case 601 => els(19) = cmdCount
                  case 701 => els(20) = cmdCount
                  case 702 => els(21) = cmdCount
                }
            }
            (device, (Array(0.1), els, Array(1.0), Array(0.1)))
        }
        .cache()
      println("readData count ", readData.count())

      val pvData = ctx.sql(
        """
          |SELECT device,content_type,read_pv
          |FROM rpt_qukan.rpt_qukan_device_content_type_read_pv
          |WHERE thedate="%s" AND content_type IS NOT NULL
        """.stripMargin.format(dayBefore)).rdd
        .map {
          x =>
            val device = x.getString(0)
            val title = x.getString(1)
            val pv = x.getInt(2).toDouble
            (device, (title, pv))
        }
        .groupByKey()
        .map {
          x =>
            val device = x._1
            var els = new Array[Double](37)
            x._2.map {
              y =>
                y._1 match {
                  case "奇闻" => els(0) += y._2
                  case "情感" => els(1) += y._2
                  case "育儿" => els(2) += y._2
                  case "三农" => els(3) += y._2
                  case "搞笑" => els(4) += y._2
                  case "故事" => els(5) += y._2
                  case "养生" => els(6) += y._2
                  case "其他" => els(7) += y._2
                  case "百科" => els(8) += y._2
                  case "历史" => els(9) += y._2
                  case "美食" => els(10) += y._2
                  case "军事" => els(11) += y._2
                  case "时尚" => els(12) += y._2
                  case "体育" => els(13) += y._2
                  case "科技" => els(14) += y._2
                  case "星座" => els(15) += y._2
                  case "汽车" => els(16) += y._2
                  case "励志" => els(17) += y._2
                  case "财经" => els(18) += y._2
                  case "旅游" => els(19) += y._2
                  case "游戏" => els(20) += y._2
                  case "推荐" => els(21) += y._2
                  case "国际" => els(22) += y._2
                  case "设计" => els(23) += y._2
                  case "动漫" => els(24) += y._2
                  case "电影" => els(25) += y._2
                  case "宠物" => els(26) += y._2
                  case "收藏" => els(27) += y._2
                  case "相关推荐" => els(28) += y._2
                  case "发现" => els(29) += y._2
                  case "文化" => els(30) += y._2
                  case "社会" => els(31) += y._2
                  case "娱乐" => els(32) += y._2
                  case "萌物" => els(33) += y._2
                  case "酷男" => els(34) += y._2
                  case "女神" => els(35) += y._2
                  case "才艺" => els(36) += y._2
                  case _ =>
                }
            }
            (device, (Array(0.1), Array(0.1), els, Array(0.1)))
        }

      println("pvData count ", pvData.count())

      var phoneNetData = ctx.sql(
        """
          |SELECT uid,network,ext["phone_level"].int_value
          |FROM dl_cpc.cpc_union_log
          |WHERE `date`="%s" AND ext["phone_level"].int_value>=0 AND network>=0
        """.stripMargin.format(dayBefore))
        .rdd
        .map {
          x =>
            val device = x.getString(0)
            val network = x.getInt(1)
            val plevel = x.getInt(2)
            (device, (network, plevel))
        }
        .groupByKey()
        .map {
          x =>
            var network = new Array[Double](5)
            var plevel = new Array[Double](5)

            x._2.foreach {
              y =>
                network(y._1) += 1.0
                plevel(y._2) += 1.0
            }
            (x._1, (Array(0.1), Array(0.1), Array(0.1), network ++ plevel))
        }

      var xdata = xAgeData //agelogpvdata
        .union(hourData)
        .union(pvData)
        .union(readData)
        .union(phoneNetData)
        .reduceByKey {
          (a, b) =>
            var hour = a._1
            var read = a._2
            var pv = a._3
            var plnet = a._4

            if (hour.length == 1) {
              hour = b._1
            }

            if (read.length == 1) {
              read = b._2
            }

            if (pv.length == 1) {
              pv = b._3
            }

            if (plnet.length == 1) {
              plnet = b._4
            }

            (hour, read, pv, plnet)
        }
        .filter {
          x =>
            val device = x._1
            val hour = x._2._1
            val read = x._2._2
            val pv = x._2._3
            val plnet = x._2._4
            ((device.length > 0) && (hour.length == 24) && (read.length == 22) && (pv.length == 37) && (plnet.length == 10))
        }
        .map {
          x =>
            val device = x._1
            val hour = x._2._1
            val read = x._2._2
            val pv = x._2._3
            val plnet = x._2._4
            (device, hour ++ read ++ pv ++ plnet)
        }
        .filter {
          x =>
            (x._1.length > 0 && x._2.size == HASHSUM)
        }
        .cache()

      //保存昨天数据
      xdata
        .map {
          x =>
            "%s\t%s".format(x._1, x._2.mkString(","))
        }
        .saveAsTextFile("/user/cpc/wl/work/UserAgeData/%s".format(dayBefore))
    }
    //    //-----------------------------------------
    var fileNameArr = new Array[String](40)

    //生成数据时间范围数据
    for (i <- 1 to 40) {
      val cal = Calendar.getInstance()
      cal.add(Calendar.DATE, -i)
      val day = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      fileNameArr(i - 1) = day
    }

    println("fileNameArr", fileNameArr.mkString(","))
    //val fileStr = "2017-10-09,2017-10-08,2017-10-07,2017-10-06,2017-10-05,2017-10-04,2017-10-03,2017-10-02,2017-10-01,2017-09-30,2017-09-29,2017-09-28,2017-09-27,2017-09-26,2017-09-25,2017-09-24,2017-09-23,2017-09-22,2017-09-21,2017-09-20,2017-09-19,2017-09-18,2017-09-17,2017-09-16,2017-09-15,2017-09-14,2017-09-13,2017-09-12,2017-09-11,2017-09-10,2017-09-09,2017-09-08,2017-09-07,2017-09-06,2017-09-05,2017-09-04,2017-09-03,2017-09-02,2017-09-01,2017-08-30"

    val oldData = ctx
      .sparkContext
      .textFile("/user/cpc/wl/work/UserAgeData/{%s}".format(fileNameArr.mkString(",")))
      .map {
        x =>
          val arr = x.split("\t")
          var device = ""
          var els = Array(0.1)
          if (arr.length == 2) {
            device = arr(0)
            els = arr(1).split(",").map(_.toDouble)
          }
          (device, (device, els))
      }
      .filter(x => (x._1.length > 0 && x._2._2.size == HASHSUM))
      .reduceByKey {
        (a, b) =>
          var device = a._1
          var els = a._2

          for (i <- 0 to HASHSUM - 1) {
            els(i) += b._2(i)
          }
          (device, els)
      }
      .map {
        x =>
          (x._2._1, (-1, x._2._2))
      }
      .repartition(50)
      .cache()

    println("oldData count", oldData.count())

    val ydata = xAgeData
      .map {
        x =>
          (x._1, (1, x._2._1))
      }
      .union(oldData)
      .reduceByKey {
        (a, b) =>
          var tag = a._1
          var els = a._2
          if (a._1 == -1) {
            tag = b._1
          }
          if (els.length == 1) {
            els = b._2
          }
          (tag, els)
      }
      .filter(x => ((x._2._1 != -1) && (x._2._2.length == HASHSUM)))
      .map {
        x =>
          val device = x._1
          val els = Vectors.dense(x._2._2)
          (device, els)
      }
      .repartition(50)
      .cache()

    println("ydata count ", ydata.count())

    //    ydata.map {
    //      x =>
    //        "%s\t%s".format(x._1, x._2.toArray.mkString(","))
    //    }
    //      .saveAsTextFile("/user/cpc/wl/test/GetAddAge-data")

    //    val ydata = ctx.sparkContext.textFile("/user/cpc/wl/test/GetAddAge-data")
    //      .map {
    //        x =>
    //          val arr = x.split("\t")
    //          if (arr.length != 2) {
    //            ("", Vectors.dense(Array(0.1)))
    //          } else {
    //            val device = arr(0).toString
    //            var els = arr(1).toString.split(",").map(_.toDouble)
    //            (device, Vectors.dense(els))
    //          }
    //      }
    //      .filter(_._1 != "")

    val mmodel = MultilayerPerceptronClassificationModel.load("/user/cpc/wl/work/GetTrainUserAgeModelZ-9000-%d".format(1))

    val result = ydata.map {
      x =>
        val xmethod = classOf[MultilayerPerceptronClassificationModel].getMethod("predict", classOf[Vector])
        xmethod.setAccessible(true)
        val field = classOf[MultilayerPerceptronClassificationModel].getDeclaredField("mlpModel")
        field.setAccessible(true)
        val method = field.getType.getMethod("predict", classOf[Vector])
        method.setAccessible(true)

        val maxnum = method.invoke(field.get(mmodel), x._2).asInstanceOf[DenseVector].toArray.sortWith(_ > _).head
        (x._1, maxnum, method.invoke(field.get(mmodel), x._2).asInstanceOf[DenseVector], xmethod.invoke(mmodel, x._2).asInstanceOf[Double])
    }
      .filter(x => x._2 >= 0.64)
      .repartition(50)
      .cache()

    //println("result >= 0.65 count ", result.count())
    //    println("result >= 0.65 0 count ", result.filter(_._4.toInt == 0).count())
    //    println("result >= 0.65 1 count ", result.filter(_._4.toInt == 1).count())
    //    println("result >= 0.65 2 count ", result.filter(_._4.toInt == 2).count())
    //  val sum = result.mapPartitions {
    //    p =>
    //      var n1 = 0
    //      p.foreach {
    //        x =>
    //          n1 += x
    //      }
    //      Seq((0, n1)).iterator
    //  }
    //  sum.reduceByKey((a, b) => a + b).take(5).foreach(println)
    //------------------------------------------------
    //    for (a <- 30 to 99) {
    //      val tmp = result.filter(x => (x._2 * 100).toInt >= a).cache()
    //      val count = tmp.count()
    //      val age0 = tmp.filter(_._4.toInt == 0).count()
    //      val age1 = tmp.filter(_._4.toInt == 1).count()
    //      val age2 = tmp.filter(_._4.toInt == 2).count()
    //      println(a, count, age0, age1, age2)
    //    }
    //println("result >=0.62 is ok count ", result.filter(x => x._4.toInt == x._5).count())
    //
    //    for (a <- 0 to 99) {
    //      val tmp = result.filter(x => (x._2 * 100).toInt >= a).cache()
    //      val count = tmp.count()
    //      val ok = tmp.filter(x => (x._4.toInt == x._5)).count()
    //      println(a, count, ok)
    //    }

    //-----------------------------------------

    // 三分类对应 adv 6分类
    //          1: 小于18 2:18-23
    //          3:24-30 4:31-40
    //          5:41-50 6: >50
    val sum = result
      .map {
        x =>
          val device = x._1
          val age = x._4.toInt
          val num = (new util.Random).nextInt(2)
          var randomAge = 0
          if (age == 0) {
            randomAge = num + 1
          } else if (age == 1) {
            randomAge = num + 3
          } else if (age == 2) {
            randomAge = num + 5
          }
          (device, randomAge)
      }
      .mapPartitions {
        p =>
          val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))

          var n1 = 0
          var n2 = 0

          var age1 = 0
          var age2 = 0
          var age3 = 0
          var age4 = 0
          var age5 = 0
          var age6 = 0

          p.foreach {
            row =>
              val key = row._1 + "_UPDATA"
              val age = row._2

              if (age == 1) {
                age1 += 1
              } else if (age == 2) {
                age2 += 1
              } else if (age == 3) {
                age3 += 1
              } else if (age == 4) {
                age4 += 1
              } else if (age == 5) {
                age5 += 1
              } else if (age == 6) {
                age6 += 1
              }
              var user = UserProfile.newBuilder().setDevid(row._1)

              val buffer = redis.get[Array[Byte]](key).getOrElse(null)

              if (buffer != null) {
                n1 += 1
                user = UserProfile.parseFrom(buffer).toBuilder
              } else {
                n2 += 1
              }
              user.setAge(age)
              redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
          }
          Seq((0, n1), (1, n2), (2, age1), (3, age2), (4, age3), (5, age4), (6, age5), (7, age6)).iterator
      }

    var n1 = 0
    var n2 = 0
    var age1 = 0
    var age2 = 0
    var age3 = 0
    var age4 = 0
    var age5 = 0
    var age6 = 0

    sum.reduceByKey((a, b) => a + b)
      .take(8)
      .foreach {
        x =>
          if (x._1 == 0) {
            n1 += x._2
          } else if (x._1 == 1) {
            n2 += x._2
          } else if (x._1 == 2) {
            age1 += x._2
          } else if (x._1 == 3) {
            age2 += x._2
          } else if (x._1 == 4) {
            age3 += x._2
          } else if (x._1 == 5) {
            age4 += x._2
          } else if (x._1 == 6) {
            age5 += x._2
          } else if (x._1 == 7) {
            age6 += x._2
          }
      }

    println("small tool GetAddAge n1: %d ,n2: %d,a1: %d,a2: %d,a3: %d,a4: %d,a5: %d,a6: %d".format(n1, n2
      , age1, age2, age3, age4, age5, age6))
    ctx.stop()


    //---------------------------------------------------------------------
    //刷用户表已存在年龄的用户 不要乱动
    //    val oldAgeData = ctx
    //      .sparkContext
    //      .textFile("/user/cpc/wl/work/small-tool-UserAgeBase")
    //      .map {
    //        x =>
    //          val arr = x.split("\t")
    //          var device = ""
    //          var age = -1
    //          if (arr.length == 2) {
    //            device = arr(0)
    //            age = arr(1).toInt
    //          }
    //          (device, age)
    //      }
    //      .filter(_._2 != -1)
    //
    //    val sum = oldAgeData
    //      .mapPartitions {
    //        p =>
    //          val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
    //
    //          var n1 = 0
    //          var n2 = 0
    //
    //          var age1 = 0
    //          var age2 = 0
    //          var age3 = 0
    //          var age4 = 0
    //          var age5 = 0
    //          var age6 = 0
    //
    //          p.foreach {
    //            row =>
    //              val key = row._1 + "_UPDATA"
    //              val age = row._2
    //
    //              if (age == 1) {
    //                age1 += 1
    //              } else if (age == 2) {
    //                age2 += 1
    //              } else if (age == 3) {
    //                age3 += 1
    //              } else if (age == 4) {
    //                age4 += 1
    //              } else if (age == 5) {
    //                age5 += 1
    //              } else if (age == 6) {
    //                age6 += 1
    //              }
    //              var user = UserProfile.newBuilder().setDevid(row._1)
    //
    //              val buffer = redis.get[Array[Byte]](key).getOrElse(null)
    //
    //              if (buffer != null) {
    //                n1 += 1
    //                user = UserProfile.parseFrom(buffer).toBuilder
    //              } else {
    //                n2 += 1
    //              }
    //
    //              //else {
    //              ////                var user = UserProfile.newBuilder().setDevid(row._1)
    //              ////                user.setAge(age)
    //              //                  redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
    //              //
    //              //              }
    //              user.setAge(age)
    //              redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
    //          }
    //          Seq((0, n1), (1, n2), (2, age1), (3, age2), (4, age3), (5, age4), (6, age5), (7, age6)).iterator
    //      }
    //
    //    var n1 = 0
    //    var n2 = 0
    //    var age1 = 0
    //    var age2 = 0
    //    var age3 = 0
    //    var age4 = 0
    //    var age5 = 0
    //    var age6 = 0
    //    sum.reduceByKey((a, b) => a + b)
    //      .take(8)
    //      .foreach {
    //        x =>
    //          if (x._1 == 0) {
    //            n1 += x._2
    //          } else if (x._1 == 1) {
    //            n2 += x._2
    //          } else if (x._1 == 2) {
    //            age1 += x._2
    //          } else if (x._1 == 3) {
    //            age2 += x._2
    //          } else if (x._1 == 4) {
    //            age3 += x._2
    //          } else if (x._1 == 5) {
    //            age4 += x._2
    //          } else if (x._1 == 6) {
    //            age5 += x._2
    //          } else if (x._1 == 7) {
    //            age6 += x._2
    //          }
    //      }
    //
    //    println("small tool GetAddAge n1: %d ,n2: %d,a1: %d,a2: %d,a3: %d,a4: %d,a5: %d,a6: %d".format(n1, n2
    //      , age1, age2, age3, age4, age5, age6))
    //    ctx.stop()
  }
}
