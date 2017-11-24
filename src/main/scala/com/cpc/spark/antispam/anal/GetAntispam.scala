package com.cpc.spark.antispam.anal

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by wanli on 2017/8/4.
  */
object GetAntispam {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger().setLevel(Level.WARN)

    val dateDay = args(0)

    val ctx = SparkSession.builder()
      .appName("cvr report")
      .enableHiveSupport()
      .getOrCreate()
   val sql =  """
             SELECT uid,isclick,isshow,adslot_type,hour
             from dl_cpc.cpc_union_log where ext['antispam'].int_value =1
             and `date`="%s"
           """.stripMargin.format(dateDay)
    println("sql:"+sql)
    val rdd = ctx.sql(sql).rdd
      .map {
        x =>
          val uid : String =  x(0).toString()
          val click : Int = x(1).toString().toInt
          val show : Int = x(2).toString().toInt
          val adslotType : Int = x(3).toString().toInt
          val hour  = x(4).toString()
          (uid,click, show,adslotType,hour)
        }.cache()
    val rddfilter = rdd.filter(x => x._2 == 1).map(x => (x._1, x._4)).distinct().reduceByKey((x,y) => x+y)

    val rddx = rdd.map(x => (x._1, (x._2, x._3, x._4, x._5)))
    val xx = rdd.filter(x => x._2 == 1 && x._4 ==1).map(x => (x._1,1)).distinct()
    val rddfilter2 = xx.join(rddx).map{
      case (uid,(x,(click, show,adslotType,hour))) =>
          (uid,click, show,adslotType,hour)
    }


    val rdd2  = rddfilter.filter(x => x._2 == 1)
    val rdd3  = rddfilter.filter(x => x._2 == 2)
    val rdd4  = rddfilter.filter(x => x._2 == 3)

    println("all uid  " + rdd.map(x => x._1).distinct().count())
    rdd.map{
      case (uid,click, show,adslotType,hour) =>
        (hour,(click, show))
    }.reduceByKey((x, y) =>(x._1+ y._1, x._2 +y._2)).map{
      case (hour,(click, show)) =>
        (hour, click, show,click.toDouble/show.toDouble * 100)
    }.collect().foreach(x => println("all:"+ x))

    println("all list" + rddfilter2.filter(x => x._4 ==1).map(x => (x._1)).distinct().count())

    rddfilter2.filter(x => x._4 == 1).map{
      case (uid,click, show,adslotType,hour) =>
        (hour,(click, show))
    }.reduceByKey((x, y) =>(x._1+ y._1, x._2 +y._2)).map{
      case (hour,(click, show)) =>
        (hour, click, show,click.toDouble/show.toDouble * 100)
    }.collect().foreach(x => println("all list:"+ x))

    println("only list" + rdd2.count())
    rdd.map{
      case (uid,click, show,adslotType,hour) =>
        (uid,(click, show,adslotType,hour))
    }.join(rdd2).map{
      case (uid,((click, show,adslotType,hour),adslotType2)) =>
        (hour,(click, show))
    }.reduceByKey((x, y) =>(x._1+ y._1, x._2 +y._2)).map{
      case (hour,(click, show)) =>
        (hour,click, show, click.toDouble/show.toDouble * 100)
    }.collect().foreach(x => println("only list:"+ x))

    println("only info" + rdd3.count())
    rdd.map{
      case (uid,click, show,adslotType,hour) =>
        (uid,(click, show,adslotType,hour))
    }.join(rdd3).map{
      case (uid,((click, show,adslotType,hour),adslotType2)) =>
        (hour,(click, show))
    }.reduceByKey((x, y) =>(x._1+ y._1, x._2 +y._2)).map{
      case (hour,(click, show)) =>
        (hour,click, show, click.toDouble/show.toDouble * 100)
    }.collect().foreach(x => println(" info:"+ x))

    println("list and info" + rdd4.count())
    rdd.map{
      case (uid,click, show,adslotType,hour) =>
        (uid,(click, show,adslotType,hour))
    }.join(rdd4).map{
      case (uid,((click, show,adslotType,hour),adslotType2)) =>
        ((hour,adslotType),(click, show))
    }.reduceByKey((x, y) =>(x._1+ y._1, x._2 +y._2)).map{
      case ((hour,adslotType),(click, show)) =>
        (hour,adslotType,click, show, click.toDouble/show.toDouble * 100)
    }.collect().foreach(x => println(" list and info:"+ x))
    rdd.map{
      case (uid,click, show,adslotType,hour) =>
        (uid,(click, show,adslotType,hour))
    }.join(rdd4).map{
      case (uid,((click, show,adslotType,hour),adslotType2)) =>
        ((hour),(click, show))
    }.reduceByKey((x, y) =>(x._1+ y._1, x._2 +y._2)).map{
      case ((hour),(click, show)) =>
        (hour,click, show, click.toDouble/show.toDouble * 100)
    }.collect().foreach(x => println(" list and info2:"+ x))
  }
}