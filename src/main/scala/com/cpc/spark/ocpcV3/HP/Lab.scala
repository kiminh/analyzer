package com.cpc.spark.ocpcV3.HP

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.fpm.FPGrowth

object Lab {
  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("fpg")
    val sc   = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)

    //1. 读取样本数据
    val filename = "sample_fpgrowth.txt"
    val data_path = s"hdfs://emr-cluster/warehouse/sunjianqiang/${filename}"
    val data = sc.textFile(data_path)
    val examples = data.map(_.split(" ")).cache( )

    //2. 建立模型
    //设置最小支持度
    val minSupport = 0.2
    //设置并行分区数
    val numPartition = 10
    val model = new FPGrowth()
      .setMinSupport( minSupport )
      .setNumPartitions(numPartition)
      .run(examples)

    //3. 查看所有的频繁项集，并且列出它们出现的次数
    println(s"Number of frequent itemsets: ${model.freqItemsets.count()}")
    println("model.freqItemsets type: " + model.freqItemsets.getClass.getSimpleName())
    println("model.generateAssociationRules type: " + model.generateAssociationRules())
    model.freqItemsets.collect().foreach{ itemset => println( itemset.items.mkString("[", ",", "]") + "," + itemset.freq ) }

    //4. 通过置信度筛选出推荐规则
    //antecedent：前项
    //consequent：后项
    //confidence：置信度
    val minConfidence = 0.8
    model.generateAssociationRules(minConfidence).collect()
      .foreach( rule => {
        println( rule.antecedent.mkString(",") + "-->" + rule.consequent.mkString(",") + "-->" + rule.confidence )
      })

    //查看规则生成的数量
    println(model.generateAssociationRules(minConfidence).getClass().getSimpleName() )
  }
}








