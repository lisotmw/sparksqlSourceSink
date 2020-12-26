package com.sparkss.common

import java.util

import org.apache.spark.sql.Row

import scala.collection.JavaConversions

/**
 * java类型转换器
 */
object JavaConversion {

  /**
   * java list to scala buffer
   * @param list
   * @return
   */
  def asScala(list: util.List[Object]):Row={
    val array = JavaConversions.asScalaBuffer(list)
    Row.fromSeq(array)
  }

}
