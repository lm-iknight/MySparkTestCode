package com.liming.base

import scala.beans.BeanProperty

/**
 * 功能描述: IncomeKpi.scala里用的基类
 * 作者: 李明
 * 创建时间: 2020年02月23日 15点23分35秒
 */
// extends Serializable 由于是分布式计算，因此需要进行序列化，方便在网络里传输
class OrderBean extends Serializable {
  //由于是使用阿里的fastjson，用scala写这个普通的class需加@BeanProperty ，生成getter和setter方法。如果使用java写的bean就不用加了。
  //还有一个更好的方式，就是定义一个case class，而不用这个class
  @BeanProperty
  var cid: Int = _
  @BeanProperty
  var money: Double = _
  @BeanProperty
  var longitude: Double = _
  @BeanProperty
  var latitude: Double = _

  override def toString = s"OrderBean($cid,$money,$longitude,$latitude)"
}
