package com.liming

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 功能描述: 
 * 作者: 李明
 * 创建时间: 2020年02月13日 13点08分12秒
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(config)
    //    从文件读取文本
    //    val lines: RDD[String] = sc.textFile("input/manager.log")
    //    用空格对文本进行切分
    //    val words: RDD[String] = lines.flatMap(_.split(" "))
    //    对每一个元素操作，将单词映射为元组
    //    val wordToOne: RDD[(String, Int)] = words.map((_, 1))
    //    按照key将值进行聚合，相加
    //    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
    //    将数据收集到Driver端展示
    //    val result: Array[(String, Int)] = wordToSum.collect()
    //    打印
    //    result.foreach(println)
    sc.textFile("input/manager.log").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect().foreach(println)

  }
}
