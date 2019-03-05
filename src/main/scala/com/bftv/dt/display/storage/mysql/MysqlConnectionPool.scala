package com.bftv.dt.display.storage.mysql

import java.sql.Connection

import com.bftv.dt.display.utils.ConfigUtil
import com.mchange.v2.c3p0.ComboPooledDataSource
import org.slf4j.LoggerFactory

/**
  * mysql connection pool
  *
  * @author sunliangliang 2019-03-01 https://github.com/sunliangliang9201/tv_realtime_display
  * @version 1.0
  */
class MysqlConnectionPool {

  val logger = LoggerFactory.getLogger(this.getClass)
  private val cpds: ComboPooledDataSource = new ComboPooledDataSource(true)

  val host = ConfigUtil.getConf.get.getString("mysql_host")
  val user = ConfigUtil.getConf.get.getString("mysql_user")
  val passwd = ConfigUtil.getConf.get.getString("mysql_passwd")
  val db = ConfigUtil.getConf.get.getString("mysql_db")
  val port = ConfigUtil.getConf.get.getString("mysql_port")

  try {
    cpds.setJdbcUrl("jdbc:mysql://" + host + ":" + port + "/" + db)
    cpds.setDriverClass("com.mysql.cj.jdbc.Driver")
    cpds.setUser(user)
    cpds.setPassword(passwd)
    cpds.setMaxPoolSize(10)
    cpds.setMinPoolSize(1)
    cpds.setAcquireIncrement(1)
    cpds.setMaxStatements(100)
    cpds.setInitialPoolSize(1)
  } catch {
    case e: Exception => e.printStackTrace()
  }

  /**
    * 获取连接
    * @return 连接
    */
  def getConnection: Connection = {
    try {
      return cpds.getConnection();
    } catch {
      case e: Exception => logger.error("Get the connection from pool failed ..., " + e)
        null
    }
  }
}

/**
  * 实例对象
  */
object MysqlManager {
  var mysqlManager: MysqlConnectionPool = _
  def getMysqlManager: MysqlConnectionPool = {
    synchronized {
      if (mysqlManager == null) {
        mysqlManager = new MysqlConnectionPool
      }
    }
    mysqlManager
  }
}
