package com.cpc.spark.streaming.tools

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

object SparkApp {
  val conf: Config = ConfigFactory.load()

  val logger: Logger = {
    val _logger = LogManager.getLogger(conf.getString("logger.name"))
    _logger.setLevel(Level.toLevel(conf.getString("logger.level")))
    _logger.info("Getting Logger...")
    _logger
  }

  val spark: SparkSession = {
    logger.info("Getting SparkSession...")
    val _spark = SparkSession
      .builder
      .config(new SparkConf().setAll(
        for ((key, value) <- conf.getObject("spark.conf").asScala)
          yield (key.replace('_', '.'), value.unwrapped.toString)
        )
      )
      .enableHiveSupport
      .getOrCreate
    _spark
  }
}