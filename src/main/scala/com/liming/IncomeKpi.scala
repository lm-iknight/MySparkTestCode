package com.liming

import java.lang

import com.alibaba.fastjson.{JSON, JSONException, JSONObject}
import com.liming.base.CaseOrderBean
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

/**
 * 功能描述: 一个计算的栗子,解析JSON.使用两种方式
 * 作者: 李明
 * 创建时间: 2020年02月22日 10点28分30秒
 */
object IncomeKpi {
  private val logger: Logger = LoggerFactory.getLogger(IncomeKpi.getClass)

  def main(args: Array[String]): Unit = {
    val file_path = "f://log.json"
    val isLocal = args(0).toBoolean
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName)
    if (isLocal) {
      conf.setMaster("local[*]")
    } else {
      conf.setMaster("spark://192.168.23.11:7077")
    }
    val sc = new SparkContext(conf)

    //创建RDD
    val lines: RDD[String] = sc.textFile(args(1)) //读入的文件格式是JSON，需要在POM里引入包
    //第一种方式解析JSON，用于少量数据**********************************************************************
    /*
    val cidAndMoney: RDD[(Int, Double)] = lines.map(line => {
      //定义一个元组，用于存放try里的每行的数据
      var tp = (-1, 0.0) //当某行有脏数据，返回的是(-1, 0.0)，可以进一步处理，比如过滤掉
      //使用FastJson解析
      var jsonObj: JSONObject = null
      try {
        jsonObj = JSON.parseObject(line)
        //获取JSON中的数据
        //      val oid: String = jsonObj.getString("oid")
        val cid: Integer = jsonObj.getInteger("cid").toInt //toInt是转换成scala的Int，否则就是java的Int
        val money: lang.Double = jsonObj.getDouble("money").toDouble
        tp = (cid, money)
      } catch {
        case e: JSONException => {
          logger.error("parse json error : =>" + line)
        }
      }
      //返回元组
      tp
    })
    val r: Array[(Int, Double)] = cidAndMoney.collect()
    println(r.toBuffer)
    */
    //第二种方式解析JSON，用于大量数据**********************************************************************

    val beanRdd: RDD[CaseOrderBean] = lines.map(line => {
      var bean: CaseOrderBean = null

      try {
        bean = JSON.parseObject(line, classOf[CaseOrderBean])
      } catch {
        case e: JSONException => {
          logger.error("parse json error : =>" + line)
        }
      }
      bean
    })
    //过滤有问题的数据
    val filtered: RDD[CaseOrderBean] = beanRdd.filter(_ != null)
    // 将数据转成元组，用于分组聚合
    val cidAndMoney: RDD[(Int, Double)] = filtered.map(bean => {
      val cid = bean.cid
      val money = bean.money
      (cid, money)
    })
    // 分组聚合
    val reduced: RDD[(Int, Double)] = cidAndMoney.reduceByKey(_ + _)
    println(reduced.collect().toBuffer)
    // 再创建一个RDD 读维度（也就是码表）的文件,用于编码转换
    val categoryLines: RDD[String] = sc.textFile(args(2))
    //分类RDD
    val cidAndCname: RDD[(Int, String)] = categoryLines.map(line => {
      val fields = line.split(",")
      val cid = fields(0).toInt
      val cname = fields(1)
      (cid, cname)
    })
    //将两个RDD进行join,得到一个新的RDD
    val joined: RDD[(Int, (Double, String))] = reduced.join(cidAndCname)
    println(joined.collect().toBuffer)
    //将新的RDD即join之后的数据进行处理
    val r: RDD[(String, Double)] = joined.map(t => (t._2._2, t._2._1))
    println(r.collect().toBuffer)

    //



    sc.stop()
  }
}
