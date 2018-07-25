package com.cpc.spark.log.anal

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by roydong on 25/07/2018.
  */
object IdeaRemain {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("idea remain")
      .enableHiveSupport()
      .getOrCreate() //获得sparksession

    val days = args(0).toInt
    val cal = Calendar.getInstance()
    var last: RDD[(Int, Int)] = null
    var ids: RDD[(Int, Int)] = null
    cal.add(Calendar.DATE, -days)
    for (d <- 1 to days) {
      cal.add(Calendar.DATE, 1)
      val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      val stmt =
        """
          |select ideaid from dl_cpc.cpc_union_log where `date` = "%s" and ideaid > 0
        """.stripMargin.format(date)
      println(stmt)

      ids = spark.sql(stmt).rdd.map(_.getInt(0)).distinct().map(x => (x, 1))
      val num = ids.count()
      var remain = 0l
      if (last != null) {
        remain = ids.join(last).count()
      }

      last = ids
      println(date, num, remain)
    }
  }
}
