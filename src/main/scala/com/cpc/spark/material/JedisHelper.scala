package com.cpc.spark.material

import java.io.{File, PrintWriter}

import com.alibaba.fastjson.JSON
import com.google.protobuf.util.JsonFormat
import redis.clients.jedis.Jedis
import redis.clients.jedis.ScanParams

import scala.collection.JavaConversions.asScalaBuffer
import scala.util.control.Breaks._

object JedisHelper {

  def scan(jedis: Jedis, file: File): Unit = {

    val scanParams = new ScanParams()
    var cursor = ScanParams.SCAN_POINTER_START
    val keyPrefix = "material_*"
    scanParams.`match`(keyPrefix)
    scanParams.count(1000)

    val printer = JsonFormat.printer().includingDefaultValueFields()
    val writer = new PrintWriter(file)

    breakable(
      while (true) {
        val scanResult = jedis.scan(cursor, scanParams)

        // 遍历key 查询value
        val list = scanResult.getResult
        for (key <- list) {
          val value = jedis.get(key.getBytes())
          // 反序列化 material
          val entity = material.MaterialOuterClass.Material.parseFrom(value)
          // 转化成json格式
          val jsonStr = printer.print(entity)
          val jsonObj = JSON.parseObject(jsonStr)
          jsonObj.put("redis_key", key)

          // 写出到文件
          writer.println(jsonObj.toString)
        }

        // 返回0 说明遍历完成 结束循环
        cursor = scanResult.getStringCursor
        if ("0" == cursor) {
          break
        }
      }
    )

    writer.close()
  }
}
