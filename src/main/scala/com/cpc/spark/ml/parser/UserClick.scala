package com.cpc.spark.ml.parser

import scala.collection.mutable
import scala.io.Source

/**
  * Created by roydong on 2017/6/10.
  */
trait UserClick {

  val userClk = mutable.Map[String, Int]()

  val userPV = mutable.Map[String, Int]()

  val userAdClick = mutable.Map[String, Int]()

  val userSlotClick = mutable.Map[String, Int]()

  val userSlotAdClick = mutable.Map[String, Int]()

  def loadUserClickFromFile(): Unit = {
    loadUserClickPV("/home/cpc/t/user_click/user_click_pv.txt")
    loadUserAdClick("/home/cpc/t/user_click/user_ad_click.txt")
    loadUserSlotClick("/home/cpc/t/user_click/user_slot_click.txt")
    loadUserSlotAdClick("/home/cpc/t/user_click/user_slot_ad_click.txt")
  }

  def loadUserClickPV(path: String): Unit = {
    for (line <- Source.fromFile(path, "UTF8").getLines()) {
      val row = line.split("\t")
      if (row.length == 3) {
        userClk.update(row(0), row(1).toInt)
        userPV.update(row(0), row(2).toInt)
      }
    }
  }

  def loadUserAdClick(path: String): Unit = {
    for (line <- Source.fromFile(path, "UTF8").getLines()) {
      val row = line.split("\t")
      if (row.length == 4) {
        userAdClick.update("%s-%s".format(row(0), row(1)), row(2).toInt)
      }
    }
  }

  def loadUserSlotClick(path: String): Unit = {
    for (line <- Source.fromFile(path, "UTF8").getLines()) {
      val row = line.split("\t")
      if (row.length == 4) {
        userSlotClick.update("%s-%s".format(row(0), row(1)), row(2).toInt)
      }
    }
  }

  def loadUserSlotAdClick(path: String): Unit = {
    for (line <- Source.fromFile(path, "UTF8").getLines()) {
      val row = line.split("\t")
      if (row.length == 5) {
        userSlotAdClick.update("%s-%s-%s".format(row(0), row(1), row(2)), row(3).toInt)
      }
    }
  }
}

