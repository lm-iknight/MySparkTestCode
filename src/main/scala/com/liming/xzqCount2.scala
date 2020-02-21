package com.liming

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import scala.collection.mutable.ArrayBuffer

/**
 * 功能描述: 读取行政区的文档并计算每级行政区的个数,使用的是Spark2.x的sparkSql的API
 * 作者: 李明
 * 创建时间: 2020年02月13日 15点20分26秒
 */
object xzqCount2 {
  def main(args: Array[String]): Unit = {
    val txt_file_path = "f://xzq.txt"
    val csv_file_path = "f://xzq.csv"
    val cluster_path = "hdfs://192.168.23.11:9000/input/xzq.csv"
    //Spark 2.x 版的 wordCount
    //创建sparkSession
    //    本地调试模式
    val spark = SparkSession.builder().appName("wordCount2.x").master("local[2]").getOrCreate()
    //集群模式
    //    val spark = SparkSession.builder().appName("wordCount2.x").master("spark://192.168.23.11:7077").getOrCreate()
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
    //调试时调用本地文件
    //        val csv_df: DataFrame = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").csv(csv_file_path)
    //调用HDFS上的文件,如果此文件里有中文会乱码
//    val csv_df: DataFrame = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").csv(cluster_path)

    ////调用HDFS上的文件,如果此文件里有中文用此方法
    //先定义shcema
    val mySchema: ArrayBuffer[String] = ArrayBuffer("seq","city_code", "city_name", "county_code", "county_name", "town_code", "town_name", "village_code", "village_name")
    val csv_df:DataFrame = readZhFile(spark,"TRUE",mySchema,"GBK",csv_file_path)
    //    csv_df.printSchema()
    //    把DataFrame注册成视图
    csv_df.createTempView("csv_view")
    csv_df.show(20)
    //    如果不定义列名，默认列名是  _c0  _c1  ...
    //    val df: DataFrame = csv_df.select($"_c0").groupBy("_c0").count()
    //    df.show()
    //    csv_df.show()
    //    println("@@@@@@@@@@@="+l)
    //    *******************************************************
    spark.stop()
  }

  //hdfs上的文件里如果有中文，spark读hdfs上的文件会出现乱码，因此写此方法进行转换
  def readZhFile(spark: SparkSession, headerSchema: String, mySchema: ArrayBuffer[String], code: String, file: String): DataFrame = {
    val rddArr: RDD[Array[String]] = spark.sparkContext.hadoopFile(file, classOf[TextInputFormat], classOf[LongWritable], classOf[Text]).map(
      pair => new String(pair._2.getBytes, 0, pair._2.getLength, code))
      .map(_.trim.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1))
    val fieldArr: Array[String] = rddArr.first()
    val rddRow = rddArr.filter(!_.reduce(_ + _).equals(fieldArr.reduce(_ + _))).map(Row.fromSeq(_))
    val schemaList = ArrayBuffer[StructField]()
    if ("TRUE".equals(headerSchema)) {
      for (i <- 0 until fieldArr.length) {
        println("fieldAddr(i)=" + fieldArr(i))
        schemaList.append(StructField(mySchema(i), DataTypes.StringType))
      }
    } else {
      for (i <- 0 until fieldArr.length) {
        schemaList.append(StructField(s"_c$i", DataTypes.StringType))
        println("fieldAddr(i)=" + fieldArr(i))
      }
    }
    val schema = StructType(schemaList)
    spark.createDataFrame(rddRow, schema)
  }
}
