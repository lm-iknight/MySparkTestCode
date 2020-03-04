package com.liming.util

import java.sql.{Connection, DriverManager}

/**
 * 功能描述: 连接oracle数据库的测试类
 * 作者: 李明
 * 创建时间: 2020年02月25日 13点34分33秒
 */
object DbConn {
  def main(args: Array[String]): Unit = {

    val url = "jdbc:oracle:thin:@192.168.23.3:1521/orcl"

    //驱动名称

    val driver = "oracle.jdbc.driver.OracleDriver"

    //用户名

    val username = "user_datamining"

    //密码

    val password = "Dm9527!("

    //初始化数据连接

    var connection: Connection = null

    try {

      //注册Driver

      Class.forName(driver)

      //得到连接

      connection = DriverManager.getConnection(url, username, password)

      val statement = connection.createStatement

      //执行查询语句，并返回结果

      val rs = statement.executeQuery("select id,city_code,city_name, county_code,county_name,town_code,town_name,village_code,village_name  from tbl_xzq")

      //打印返回结果

      var i = 1

      while (rs.next) {

        val city_code = rs.getString("city_code")

        val city_name = rs.getString("city_name")


        println("行号= %s,城市编号 = %s, 城市名称 = %s  ".format(i, city_code, city_name))

        i = i + 1

      }
      rs.close()
      connection.close

    } catch {
      case e: Exception => e.printStackTrace
    }

    finally { //关闭连接，释放资源
        connection.close
    }

  }
}
