package com.liming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD

/**
 * 功能描述: 读取行政区的文档并计算每级行政区的个数,使用的是Spark2.x的sparkSql的API
 * 作者: 李明
 * 创建时间: 2020年02月13日 15点20分26秒
 */
object xzqCount2 {
  def main(args: Array[String]): Unit = {
    val txt_file_path = "f://xzq.txt";
    val csv_file_path = "f://xzq.csv";
    //Spark 2.x 版的 wordCount
    //创建sparkSession
    val spark = SparkSession.builder()
      .appName("wordCount2.x")
      .master("local[2]")
      .getOrCreate()
    //    *******************************************************
    //导入隐式转换
    import spark.implicits._
    /*
        //读数据，延时加载的
        //Dataset只有一列，默认这列叫value,如果里边有汉字会是乱码，如：|��������	������	�...|
        //这个方法是读txt文件
        val txt_lines: Dataset[String] = spark.read.textFile(txt_file_path)
        //    txt_lines.show()
        //    整理数据(切分压平)
        val txt_words: Dataset[String] = txt_lines.flatMap(_.split(" "))
        //    第一个方式：把Dataset注册成视图
        txt_words.createTempView("txt_view")
        //    执行SQL(Transformation,lazy)
        val txt_result_df1: DataFrame = spark.sql("select value,count(*) counts from txt_view group by value order by counts desc")
        //    执行Action
        txt_result_df1.show()
        //    第二个方式：使用DataSet的API（DSL）
        val txt_result_df2: Dataset[Row] = txt_words.groupBy($"value" as "word").count().sort($"count" desc)
        txt_result_df2.show()
        //    第三个方式：导入聚合函数
        import org.apache.spark.sql.functions._
        val txt_result_df3: Dataset[Row] = txt_words.groupBy($"value".as("word")).agg(count("*") as "counts").orderBy($"counts" desc)
        txt_result_df3.show()

        */
    //    *******************************************************
    //这个方法是读csv文件
    import spark.implicits._
    val csv_df: DataFrame = spark.read.format("csv").option("header", "false").option("mode", "DROPMALFORMED").csv(csv_file_path)
    //    csv_df.printSchema()
//    把DataFrame注册成视图
    csv_df.createTempView("csv_view")
    csv_df.show()
    //    *******************************************************
    spark.stop()
  }

}
