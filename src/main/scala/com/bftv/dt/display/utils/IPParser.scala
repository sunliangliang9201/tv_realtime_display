package com.bftv.dt.display.utils

import org.slf4j.LoggerFactory

/**
  * 解析ip
  *
  * @author sunliangliang 2019-03-01 https://github.com/sunliangliang9201/tv_realtime_display
  * @version 1.0
  */
object IPParser {

  val logger =LoggerFactory.getLogger(this.getClass)

  def parse(ip: String, ipAreaIspCache: Array[(String, String, String, String, Long, Long)]): (String, String, String, String) ={
    if(binarySearch(ipAreaIspCache, ip2Long(ip)) != -1){
      val res = ipAreaIspCache(binarySearch(ipAreaIspCache, ip2Long(ip)))
      return (res._1, res._2, res._3, res._4)
      //中国	福建省	福州市	铁通
    }else{
      return ("-", "-", "-", "-")
    }
  }

  /**
    * ip to long
    * @param ip ip
    * @return long值
    */
  def ip2Long(ip: String): Long = {
    val fragments = ip.split("\\.")
    var ipNum = 0L
    for(i <- 0 until fragments.length){
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  /**
    * 二分法查找ip对应的long值在缓存文件中（ArratBuffer）中的索引
    * @param lines 缓存的array
    * @param ip ip
    * @return 索引值
    */
  def binarySearch(lines: Array[(String, String, String, String, Long, Long)], ip: Long): Int ={
    // 中国	福建省	福州市	铁通	3546428672	3546428679
    var low = 0
    var high = lines.length - 1
    try{
      while (low <= high) {
        val middle = (low + high) / 2
        if ((ip >= lines(middle)._5) && (ip <= lines(middle)._6))
          return middle
        if (ip < lines(middle)._5)
          high = middle - 1
        else {
          low = middle + 1
        }
      }
    }catch {
      case e: Exception => logger.error("File ip_area_isp.txt include error format data ..., " + e)
    }
    return -1
  }
}
