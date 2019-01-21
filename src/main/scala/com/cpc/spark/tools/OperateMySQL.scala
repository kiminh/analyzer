package com.cpc.spark.tools

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.spark.sql._
import com.typesafe.config.ConfigFactory

/**
  * @author Jinbao
  * @date 2019/1/19 15:38
  */
object OperateMySQL {
    /**
      * 针对Report2进行sql执行，一般用于删除语句
      * @param sql 执行语句的SQL
      */
    def update(sql:String): Unit = {
        val conf = ConfigFactory.load()
        val url = conf.getString("mariadb.report2_write.url")
        val driver = conf.getString("mariadb.report2_write.driver")
        val username = conf.getString("mariadb.report2_write.user")
        val password = conf.getString("mariadb.report2_write.password")
        var connection: Connection = null
        try {
            Class.forName(driver)
            connection = DriverManager.getConnection(url, username, password)
            val statement = connection.createStatement
            val rs = statement.executeUpdate(sql)
            println(s"execute $sql success!")
        }
        catch {
            case e: Exception => e.printStackTrace
        }
        //关闭连接，释放资源
        connection.close
    }

    /**
      * 将一个DataFrame插入Report2中的报表中去
      * @param data
      */
    def insert(data: DataFrame, table: String):Unit = {
        val conf = ConfigFactory.load()
        val mariadb_write_prop = new Properties()
        val mariadb_write_url = conf.getString("mariadb.report2_write.url")
        mariadb_write_prop.put("user", conf.getString("mariadb.report2_write.user"))
        mariadb_write_prop.put("password", conf.getString("mariadb.report2_write.password"))
        mariadb_write_prop.put("driver", conf.getString("mariadb.report2_write.driver"))

        data.write.mode(SaveMode.Append)
          .jdbc(mariadb_write_url, table,mariadb_write_prop)
        println(s"insert into $table success!")
    }
}
