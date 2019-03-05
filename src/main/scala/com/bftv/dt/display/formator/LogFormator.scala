package com.bftv.dt.display.formator

/**
  * desc
  *
  * @author sunliangliang 2018-09-28 https://github.com/sunliangliang9201/tvtest_streaming
  * @version 1.0
  */
trait LogFormator extends Serializable{

  def format(logStr: String, ipAreaIspCache: Array[(String, String, String, String, Long, Long)], fields: Array[String]): String

}
