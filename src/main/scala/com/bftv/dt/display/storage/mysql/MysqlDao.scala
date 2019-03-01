package com.bftv.dt.display.storage.mysql

import com.bftv.dt.display.model.SSKeyConf
import com.bftv.dt.display.utils.ConfigUtil
import org.slf4j.LoggerFactory

/**
  * 连接mysql执行各类sql操作
  *
  * @author sunliangliang 2019-03-01 https://github.com/sunliangliang9201/tv_realtime_display
  * @version 1.0
  */
object MysqlDao {

  val logger = LoggerFactory.getLogger(this.getClass)

  //测试用，线上用连接池
  val host = ConfigUtil.getConf.get.getString("mysql_host")
  val user = ConfigUtil.getConf.get.getString("mysql_user")
  val passwd = ConfigUtil.getConf.get.getString("mysql_passwd")
  val db = ConfigUtil.getConf.get.getString("mysql_db")
  val port = ConfigUtil.getConf.get.getString("mysql_port")

  def getSSConf(ssKey: String): SSKeyConf ={
    var ssKeyConf = null

    ssKeyConf
  }
}
