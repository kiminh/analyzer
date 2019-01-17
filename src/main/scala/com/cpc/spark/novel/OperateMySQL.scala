package com.cpc.spark.novel

import com.typesafe.config.ConfigFactory
import java.sql.{ Connection, DriverManager }
/**
  * @author Jinbao
  * @date 2018/12/17 10:46
  */
object OperateMySQL {
    def del(sql:String): Unit = {
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
            println("delete success!")
        }
        catch {
            case e: Exception => e.printStackTrace
        }
        //关闭连接，释放资源
        connection.close
    }

    def main(args: Array[String]): Unit = {
        val date = args(0)
        val sql = s"delete from report2.report_novel_evaluation where `date` = '$date'"

        println(sql)

        del(sql)
    }
}
