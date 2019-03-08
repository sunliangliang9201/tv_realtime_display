package com.bftv.dt.display.Sinks

import java.sql.{Connection, PreparedStatement}

import com.bftv.dt.display.storage.mysql.MysqlManager
import org.apache.spark.sql.{ForeachWriter, Row}

/**
  * mysql sink 实现2
  *
  * @author sunliangliang 2019-03-01 https://github.com/sunliangliang9201/tv_realtime_display
  * @version 1.0
  */
class MysqlSink2 extends ForeachWriter[Row]{

  var conn: Connection = null

  var ps: PreparedStatement = null

  override def open(partitionId: Long, epochId: Long): Boolean = {
    conn = MysqlManager.getMysqlManager.getConnection
    true
  }

  override def process(value: Row): Unit = {
    ps = conn.prepareStatement("replace into tv_position(window, appkey, province, city, count) values(?, ?, ?, ?, ?)")
    ps.setString(1, value(0).toString)
    ps.setString(2, value(1).toString)
    ps.setString(3, value(2).toString)
    ps.setString(4, value(3).toString)
    ps.setInt(5, Integer.parseInt(value(4).toString))
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
