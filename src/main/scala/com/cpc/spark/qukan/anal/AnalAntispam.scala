package com.cpc.spark.qukan.anal


import org.apache.spark.sql.SparkSession

/**
  * Created by Roy on 2017/5/5.
  */
object AnalAntispam {


  def main(argv: Array[String]): Unit = {
    if (argv.length < 1) {
      println("usage , binary <date1>")
      System.exit(1)
    }
    val date1 : String = argv(0)
    val ctx = SparkSession.builder()
      .appName("test read")
      .enableHiveSupport()
      .getOrCreate()
    val sqltext1 =  "select member_id,sum(coin) from  gobblin.qukan_p_gift_v2 where day = \"" + date1 + "\" and type = 300 group by member_id "
    println("sqltext1" + sqltext1)
    val result1 = ctx.sql(sqltext1).rdd.map{
      x =>
        val member : String = x(0).toString()
        val coin : Long = x(1).toString().toLong
        (member,coin)
    }

    result1.take(10).foreach(x => println("result12:"+ x))
    val result5 = result1.map {
      case (member,coin) =>
        var tag:String =""
        if(coin<0){
          tag = "<0"
        }else if (coin == 0){
          tag = "0"
        }else if (coin<10){
          tag = "0-10"
        }else if (coin == 10){
          tag = "10"
        }else if (coin<=50){
          tag = "11-50"
        }else if (coin<= 100) {
          tag = "51-100"
        } else if (coin<= 200) {
          tag = "101-200"
        } else if (coin<= 300) {
          tag = "201-300"
        } else if (coin<= 400) {
          tag = "301-400"
        }else if (coin <= 500){
          tag = "401-500"
        } else if (coin<=1000) {
          tag = "501-1000"
        } else if (coin<=2000) {
          tag = "1001-2000"
        } else if (coin<=5000) {
          tag = "2001-5000"
        } else if (coin<=10000) {
          tag = "5001-10000"
        }else {
          tag = ">10000"
        }
        (tag, 1)
    }
    result5.take(100).foreach(x => println("result5:"+ x))
    result5.reduceByKey{
      case (x,y) =>
       x+y
    }.collect().foreach(println)
    ctx.stop()
  }
}
