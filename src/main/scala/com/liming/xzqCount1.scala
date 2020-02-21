package com.liming


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import scala.language.postfixOps

/**
 * 功能描述: 读取行政区的文档并计算每级行政区的个数,使用的是Spark1.x的sparkSql的API,
 * API虽然已经废弃了，搂搂也是好的
 * 作者: 李明
 * 创建时间: 2020年02月14日 21点57分50秒
 */
object xzqCount1 {

  /*
    // 此方式是与case class 进行关联
    def main(args: Array[String]): Unit = {
      val txt_file_path = "f://xzq.txt";
      //    本地调试，使用所有CPU核心
      val conf: SparkConf = new SparkConf().setAppName("wordCount1.x").setMaster("local[*]")
      //    创建SparkSQL的连接（程序执行的入口）
      val sc = new SparkContext(conf)
      //    SparkContext这种方式只能创建普通的RDD，不能创建特殊的RDD(DataFrame)
      //    将SparkContext包装进而增强
      val SQLContext = new SQLContext(sc)
      //    创建特殊的RDD（DataFrame），就是有schema信息的RDD
      //    先有一个普通的RDD，然后再关联上schema，进而转成DataFrame
      //    val lines: RDD[String] = sc.textFile("hdfs://192.168.23.11:9000/person")
      val lines: RDD[String] = sc.textFile(txt_file_path)
      //    将数据进行整理
      val colRDD: RDD[Cols] = lines.map(line => {
        val fields = line.split(" ")
        val col1 = fields(0)
        val col2 = fields(1)
        val col3 = fields(2)
        Cols(col1, col2, col3)
      })
      //    该RDD装的是Cols类型的数据，有了shcma信息，但还是一个RDD
      //    将RDD转换成DataFrame
      //    导入隐式转换
      import SQLContext.implicits._
      val bdf: DataFrame = colRDD.toDF
      //    变成DataFrame后就可以使用两证API进行编程了
      // 把DataFrame注册成临时表
      bdf.registerTempTable("t_bdf")
      // 开始书写SQL(SQL方法其实是Transformation)
      val result: DataFrame = SQLContext.sql("select * from t_bdf order by col1")
      // 查看结果(触发Action)
      result.show()
      // 释放资源
      sc.stop()
    }
    */
  // 此方式是用SparkSQL的Row
  def main(args: Array[String]): Unit = {
    val txt_file_path = "f://xzq.txt";
    //    本地调试，使用所有CPU核心
    val conf: SparkConf = new SparkConf().setAppName("wordCount1.x").setMaster("local[*]")
    //    创建SparkSQL的连接（程序执行的入口）
    val sc = new SparkContext(conf)
    //    SparkContext这种方式只能创建普通的RDD，不能创建特殊的RDD(DataFrame)
    //    将SparkContext包装进而增强
    val SQLContext = new SQLContext(sc)
    //    创建特殊的RDD（DataFrame），就是有schema信息的RDD
    //    先有一个普通的RDD，然后再关联上schema，进而转成DataFrame
    //    val lines: RDD[String] = sc.textFile("hdfs://192.168.23.11:9000/person")
    val lines: RDD[String] = sc.textFile(txt_file_path)
    //    将数据进行整理
    val rowRDD: RDD[Row] = lines.map(line => {
      val fields = line.split(" ")
      val col1 = fields(0)
      val col2 = fields(1)
      val col3 = fields(2)
      Row(col1, col2, col3)
    })
    // 结构类型，其实就是表头，用于描述DataFrame
    val schema: StructType = StructType(List(
      StructField("col1", StringType, true),
      StructField("col2", StringType, true),
      StructField("col3", StringType, true)
    ))

    // 将RowRDD关联schema
    val bdf: DataFrame = SQLContext.createDataFrame(rowRDD, schema)

    //    变成DataFrame后就可以使用两种API进行编程了（sql的API，DataFrame的API）
    //******************************************************************
    /*
        //使用sql的API
        // 把DataFrame注册成临时表
        bdf.registerTempTable("t_bdf")
        // 开始书写SQL(SQL方法其实是Transformation),
        val result: DataFrame = SQLContext.sql("select * from t_bdf order by col1")
        // 查看结果(触发Action)
        result.show()
        */
    //******************************************************************
    //使用 DataFrame的API，不需要注册成临时表
    val res1: DataFrame = bdf.select("col1","col2","col3")
    import SQLContext.implicits._   //必须导入隐式转换，否则下边报错
    val res2: Dataset[Row] = res1.orderBy($"col1" desc,$"col2" asc)
    res2.show()


    //******************************************************************
    // 释放资源
    sc.stop()
  }
}

case class Cols(col1: String, col2: String, col3: String)
