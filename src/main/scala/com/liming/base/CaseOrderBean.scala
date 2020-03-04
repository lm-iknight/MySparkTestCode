package com.liming.base

/**
 * 功能描述: 用此case class 的好处：1.已经实现了序列化接口。2.不用new。3。可以进行模式匹配，给相应的属性赋值
 *           里边的属性的名字必须与JSON里保持一致，否则无法赋值
 * 作者: 李明
 * 创建时间: 2020年02月23日 15点34分14秒
 */
case class CaseOrderBean(cid:Int,money:Double,longitude:Double,latitude:Double)
