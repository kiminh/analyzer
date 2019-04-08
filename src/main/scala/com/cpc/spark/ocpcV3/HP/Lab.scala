package com.cpc.spark.ocpcV3.HP

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.fpm.FPGrowth
import java.util.Date

object Lab {
  def main(args: Array[String]): Unit = {
    val date = args(0).toString
    val spark = SparkSession.builder().appName("Lab").enableHiveSupport().getOrCreate()
    import spark.implicits._

    val baseData = getBaseData(spark, date).map(x => x._2.distinct)
    val t6 = new Date()
    println("T6 is " + t6)
    print("baseData has " + baseData.count() + " elements" )
    val t7 = new Date()
    println("T7 is " + t7)

    val minSupport = 0.4
    val numPartition = 10
    val model = new FPGrowth().setMinSupport(minSupport).setNumPartitions(numPartition).run(baseData)
    val t8 = new Date()
    println("T8 is " + t8)

    val freqItemsets = model.freqItemsets.persist()
    val numFreqItemsets = freqItemsets.count()
    println("Number of frequent itemsets: "+ numFreqItemsets )
    freqItemsets.take(30).foreach{ itemset => println( itemset.items.mkString("{", ",", "}") + ", " + itemset.freq ) }

    val minConfidence = 0.8
    val associationRules = model.generateAssociationRules(minConfidence)
    val numAssociationRules = associationRules.count()
    println("Number of association rules: "+ numAssociationRules )
    associationRules.take(30)
      .foreach{ rule => println(rule.antecedent.mkString( "{", ",", "}") + " => " + rule.consequent.mkString(",") + " : 1.confidence" + rule.confidence + "; 2.lift" + rule.lift) }



  }

  def getBaseData(spark: SparkSession, date: String) = {
    import spark.implicits._
    val sqlRequest =
      s"""
         |select
         | uid,
         | concat_ws(',', app_name) as pkgs
         | from dl_cpc.cpc_user_installed_apps a
         |where load_date = '$date'
       """.stripMargin

    println(sqlRequest)
    val t1 = new Date()
    println("T1 is " + t1)
    val df1 = spark.sql(sqlRequest).rdd
      .map(x => (x.getAs[String]("uid"), x.getAs[String]("pkgs").split(",") ))
      .flatMap(x => {
        val uid = x._1
        val pkgs = x._2
        val lb = scala.collection.mutable.ListBuffer[UidComb]()
        for (comb <- pkgs) {
          lb += UidComb(uid, comb)
        }
        lb.distinct
      }).toDF("uid", "comb").rdd
      .map(x => (x.getAs[String]("uid"), x.getAs[String]("comb")))
      .map(x => {
        val uid = x._1
        val arr = x._2.split("-", 2)
        if (arr.size == 2) (uid, arr(1)) else (uid, "")
      }).toDF("uid", "appName").persist()

    val t2 = new Date()
    println("T2 is " + t2)

    val apps_exception = Array("", "趣头条").mkString("('", "','", "')")
    val countLimit = 100

    val df2 = df1.rdd
      .map(x => x.getAs[String]("appName"))
      .map(x => (x, 1))
      .reduceByKey((x, y) => x + y).toDF("appName", "count")
      .filter(s"appName not in ${apps_exception} and  count >= ${countLimit}")

    val t3 = new Date()
    println("T3 is " + t3)

    val df3 = df1.join(df2, Seq("appName"), "left").select("uid", "appName", "count").filter("count is not NULL")

//    df3.write.mode("overwrite").saveAsTable("test.app_count_sjq")

    val t4 = new Date()
    println("T4 is " + t4)

    val result = df3.rdd
      .map(x => (x.getAs[String]("uid"), Array(x.getAs[String]("appName"))))
      .reduceByKey((appArr1, appArr2) => appArr1 ++ appArr2)
    val t5 = new Date()
    println("T5 is " + t5)
    result
  }

  case class UidComb(var uid: String, var comb: String)

}








