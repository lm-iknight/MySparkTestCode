package com.liming.util

/**
 * 功能描述: 跟IP有关的操作类
 * 作者: 李明
 * 创建时间: 2020年02月29日 11点22分16秒
 */
object IpUtils {
  // IP地址转换成十进制Long型
  def ip2Long(ip: String): Long = {
    val fragments: Array[String] = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  //十进制Long型转换成IP地址
  def int2IP(ipInt: Long): String = {
    val sb: StringBuilder = new StringBuilder
    sb.append((ipInt >> 24) & 0xFF).append(".")
    sb.append((ipInt >> 16) & 0xFF).append(".")
    sb.append((ipInt >> 8) & 0xFF).append(".")
    sb.append(ipInt & 0xFF)
    sb.toString
  }

  def main(args: Array[String]): Unit = {
    println(ip2Long("192.168.23.1")) //3232241409
    println(int2IP("3232241409".toLong))
  }


}


