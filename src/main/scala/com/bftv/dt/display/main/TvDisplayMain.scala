package com.bftv.dt.display.main

import com.bftv.dt.display.storage.mysql.MysqlDao
import org.slf4j.LoggerFactory

/**
  * 主程序入口：包含获取&更新所有配置、获取外部匹配数据如ip解析、创建structured streaming+kafka(SDF)、调用具体的逻辑处理函数s.
  *
  * @author sunliangliang 2019-02-22 https://github.com/sunliangliang9201/tv_realtime_display
  * @version 1.0
  */
object TvDisplayMain {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    //重传入参数获取key以及是否启用根据offset来获取kafka数据
    //val ssKey = args(0)
    //val flag = Integer.valueOf(args(1))
    val ssKey = "TvDisplay"
    val flag = 0
    val ssKeyConf = MysqlDao.getSSConf(ssKey)


  }
}
