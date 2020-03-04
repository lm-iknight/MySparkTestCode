package com.liming.util

import scala.collection.mutable.ArrayBuffer

/**
 * 功能描述: 
 * 作者: 李明
 * 创建时间: 2020年02月21日 16点38分30秒
 */
object ArrayBufferUse {

  /**
   * 与Array区别：
   * 1、Array是不可变的，不能直接地对其元素进行删除操作，只能通过重赋值或过滤生成新的Array的方式来删除不要的元素
   * 2、ArrayBuffer是可变的，提供了很多元素的操作，包括删除的操作
   * 他们相互转化很方便，调用toArray 、toBuffer方法即可
   */
  object ArrayBufferUse {

    def main(args: Array[String]): Unit = {

      val b = ArrayBuffer[Int]()
      b += 1;
      b += (2, 3, 4, 5, 6)
      b.remove(1) // 删除元素
      println(b.mkString(","))


      // 变成Array
      val ary = b.toArray
      println(ary.mkString(","))

      println(ary.sum)
      println(ary.max)
      println(ary.min)
      println(ary.toBuffer.toString)

      // yield
      var b1 = for (ele <- ary) yield ele * ele
      println(b1.mkString(","))

      val ab = ArrayBuffer[Int]()
      ab += (2, 3, 4, 5, 6)

      val c = for (ele <- ab) yield ele * ele
      println(c.mkString(","))

      // 找出来偶数
      val d1 = for (ele <- ab if ele % 2 == 0) yield ele * ele
      println(d1.mkString(","))

      // 函数式编程 _ 表示元素
      println(d1.filter(_ % 2 == 0).map(_ * 3).mkString(","))

      removeNegativeBad()

      removeNegativeGood()

    }

    def removeNegativeBad() = {
      val ab = new ArrayBuffer[Int]()
      ab += (1, 2, 3, 4, 5, -1, -2, -3)

      var findNegative = false
      var index = 0
      var abLen = ab.length
      while (index < abLen) {
        if (ab(index) >= 0) {
          index += 1
        } else {
          if (!findNegative) {
            findNegative = true;
            index += 1
          } else {
            ab.remove(index)
            abLen -= 1
          }
        }
      }
      println(ab.mkString(","))
    }

    def removeNegativeGood() = {
      val ab = ArrayBuffer[Int]()
      ab += (1, 2, 3, 4, -1, -2, -3, -4, -5)
      var foundNegative = false
      val keepIndex = for (i <- 0 until ab.length if !foundNegative || ab(i) >= 0) yield {
        if (ab(i) < 0) foundNegative = true
        i
      }
      for (i <- 0 until keepIndex.length) {
        ab(i) = ab(keepIndex(i))
      }
      ab.trimEnd(ab.length - keepIndex.length)
      println(ab)
    }

  }

}
