package com.cpc.spark.ml.novel

import java.text.SimpleDateFormat
import java.util.Calendar

import com.redis.serialization.Parse.Implicits._
import com.redis.RedisClient
import com.redis.serialization.Parse
import javassist.bytecode.ByteArray
import novelbookclass.Novelbookclass.SingleBookClass
import org.apache.spark.sql.SparkSession

/**
  * 生成小说三级类目
  * created time : 2019/02/19
  *
  * @author zhj
  * @version 1.0
  *
  */
object NovelClass {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val date = args(0)

    val sql =
      s"""
         |SELECT hash_id,
         |first_category_id,
         |second_category_id,
         |third_category_id
         |from bdm_book.xcx_midu_fiction_book_info where day='$date'
      """.stripMargin

    println(sql)

    val data = spark.sql(sql)

    data.foreachPartition(
      iterator => {

      val redis = new RedisClient("r-2zefa26971578ef4.redis.rds.aliyuncs.com", 6379)
      redis.auth("zrC3lReFqP0iKlwemvGDIhYA")
      redis.select(2)

      iterator.foreach (
        record => {
          val bookid = record.getString(0)
          var key = "novel_bookid_" + bookid
          val first_category_id = record.getLong(1)
          val second_category_id = record.getLong(2)
          val third_category_id = record.getLong(3)

          val buffer = redis.get[Array[Byte]](key).orNull
          if (buffer != null) {
            var book: SingleBookClass.Builder = null
            book = SingleBookClass.parseFrom(buffer).toBuilder
            book.setFirstCategoryId(first_category_id)
            book.setSecondCategoryId(second_category_id)
            book.setThirdCategoryId(third_category_id)
            redis.setex(key, 3600 * 24 * 7, book.build().toByteArray)
          } else {
            var book: SingleBookClass.Builder = null
            book.setFirstCategoryId(first_category_id)
            book.setSecondCategoryId(second_category_id)
            book.setThirdCategoryId(third_category_id)
            redis.setex(key, 3600 * 24 * 7, book.build().toByteArray)
          }
        })
        redis.disconnect
      }
      )
      println("job is done!!!!!")
    }
  }


