package com.bftv.dt.display.Sinks

import java.sql.{Connection, PreparedStatement}

import com.bftv.dt.display.storage.mysql.MysqlManager
import org.apache.spark.sql.{ForeachWriter, Row}

/**
  * mysql sink 实现
  *
  * @author sunliangliang 2019-03-01 https://github.com/sunliangliang9201/tv_realtime_display
  * @version 1.0
  */
class MysqlSink extends ForeachWriter[Row]{

  var conn: Connection = null

  var ps: PreparedStatement = null

  override def open(partitionId: Long, epochId: Long): Boolean = {
    conn = MysqlManager.getMysqlManager.getConnection
    true
  }

  override def process(value: Row): Unit = {
    ps = conn.prepareStatement("replace into tv_active(window, appkey, count) values(?, ?, ?)")
    ps.setString(1, value(0).toString)
    ps.setString(2, value(1).toString)
    ps.setInt(3, Integer.parseInt(value(2).toString))
    ps.execute()
  }

  override def close(errorOrNull: Throwable): Unit = {
    if (null != ps){
      ps.close()
    }
    if (null != conn){
      conn.close()
    }
  }
}
